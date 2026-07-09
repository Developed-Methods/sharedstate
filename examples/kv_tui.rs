use std::{
    collections::{BTreeMap, VecDeque},
    env,
    io::{self, Error, ErrorKind, Stdout, Write},
    sync::Arc,
    time::{Duration, Instant},
};

use crossterm::{
    event::{self, Event, KeyCode, KeyEvent, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use message_encoding::MessageEncoding;
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Position},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph, Wrap},
    Terminal,
};
use sharedstate::{
    cluster::node_state::NodeState,
    state::{
        deterministic_state::DeterministicState, recoverable_state::RecoverableState, subscribable_state::StateHandle,
    },
    transport::traits::{SyncConnection, SyncIO, SyncIOListener},
    SharedState, SharedStateConfig, SharedStateSettings,
};
use tokio::{
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener, TcpStream,
    },
    sync::mpsc::{self, UnboundedReceiver, UnboundedSender},
};
use tracing_subscriber::fmt::MakeWriter;
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

type KvShared = SharedState<LocalhostTcpIo, KvStore>;

const COMMAND_HELP: &str =
    "commands: help, status, get <key>, set <key> <value>, delete|del|rm <key>, list|print, quit|exit";
const PROMPT: &str = "kv> ";
const LOG_LIMIT: usize = 250;
const RENDER_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug)]
struct Args {
    leader: Option<u16>,
}

#[derive(Clone)]
struct LocalhostTcpIo {
    address: u16,
    listener: Arc<TcpListener>,
}

impl LocalhostTcpIo {
    async fn bind_ephemeral() -> io::Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let address = listener.local_addr()?.port();
        Ok(Self {
            address,
            listener: Arc::new(listener),
        })
    }
}

impl SyncIO for LocalhostTcpIo {
    type Address = u16;
    type Read = OwnedReadHalf;
    type Write = OwnedWriteHalf;

    async fn connect(&self, remote: &Self::Address) -> io::Result<SyncConnection<Self>> {
        let stream = TcpStream::connect(("127.0.0.1", *remote)).await?;
        let (read, write) = stream.into_split();
        Ok(SyncConnection {
            remote: *remote,
            read,
            write,
        })
    }
}

impl SyncIOListener for LocalhostTcpIo {
    async fn next_client(&self) -> io::Result<SyncConnection<Self>> {
        let (stream, peer) = self.listener.accept().await?;
        let (read, write) = stream.into_split();
        Ok(SyncConnection {
            remote: peer.port(),
            read,
            write,
        })
    }
}

#[derive(Clone, Debug)]
struct KvStore {
    seq: u64,
    values: BTreeMap<String, String>,
}

#[derive(Clone, Debug)]
enum KvAction {
    Set { key: String, value: String },
    Delete { key: String },
}

impl KvStore {
    fn new() -> Self {
        Self {
            seq: 1,
            values: BTreeMap::new(),
        }
    }
}

impl DeterministicState for KvStore {
    type Action = KvAction;
    type AuthorityAction = KvAction;

    fn accept_seq(&self) -> u64 {
        self.seq
    }

    fn authority(&self, action: Self::Action) -> Self::AuthorityAction {
        action
    }

    fn update(&mut self, action: &Self::AuthorityAction) {
        match action {
            KvAction::Set { key, value } => {
                self.values.insert(key.clone(), value.clone());
            }
            KvAction::Delete { key } => {
                self.values.remove(key);
            }
        }
        self.seq += 1;
    }
}

impl MessageEncoding for KvAction {
    fn write_to<T: io::Write>(&self, out: &mut T) -> io::Result<usize> {
        let mut sum = 0;
        sum += match self {
            KvAction::Set { key, value } => {
                sum += 1u16.write_to(out)?;
                sum += key.write_to(out)?;
                value.write_to(out)?
            }
            KvAction::Delete { key } => {
                sum += 2u16.write_to(out)?;
                key.write_to(out)?
            }
        };
        Ok(sum)
    }

    fn read_from<T: io::Read>(read: &mut T) -> io::Result<Self> {
        match u16::read_from(read)? {
            1 => Ok(KvAction::Set {
                key: MessageEncoding::read_from(read)?,
                value: MessageEncoding::read_from(read)?,
            }),
            2 => Ok(KvAction::Delete {
                key: MessageEncoding::read_from(read)?,
            }),
            id => Err(Error::new(ErrorKind::InvalidData, format!("unknown KvAction id {id}"))),
        }
    }
}

impl MessageEncoding for KvStore {
    fn write_to<T: io::Write>(&self, out: &mut T) -> io::Result<usize> {
        let mut sum = 0;
        sum += self.seq.write_to(out)?;
        sum += (self.values.len() as u64).write_to(out)?;
        for (key, value) in &self.values {
            sum += key.write_to(out)?;
            sum += value.write_to(out)?;
        }
        Ok(sum)
    }

    fn read_from<T: io::Read>(read: &mut T) -> io::Result<Self> {
        let seq = MessageEncoding::read_from(read)?;
        let len = u64::read_from(read)? as usize;
        let mut values = BTreeMap::new();
        for _ in 0..len {
            values.insert(MessageEncoding::read_from(read)?, MessageEncoding::read_from(read)?);
        }
        Ok(Self { seq, values })
    }
}

struct App {
    input: String,
    input_cursor: usize,
    logs: VecDeque<String>,
    should_quit: bool,
}

impl App {
    fn new() -> Self {
        Self {
            input: String::new(),
            input_cursor: 0,
            logs: VecDeque::new(),
            should_quit: false,
        }
    }

    fn log(&mut self, message: impl Into<String>) {
        self.logs.push_back(message.into());
        while self.logs.len() > LOG_LIMIT {
            self.logs.pop_front();
        }
    }

    fn insert_char(&mut self, ch: char) {
        self.input.insert(self.input_cursor, ch);
        self.input_cursor += ch.len_utf8();
    }

    fn backspace(&mut self) {
        let Some(prev) = self.prev_cursor() else {
            return;
        };
        self.input.drain(prev..self.input_cursor);
        self.input_cursor = prev;
    }

    fn delete(&mut self) {
        if self.input_cursor >= self.input.len() {
            return;
        }
        let next = self.next_cursor().unwrap_or(self.input.len());
        self.input.drain(self.input_cursor..next);
    }

    fn move_left(&mut self) {
        if let Some(prev) = self.prev_cursor() {
            self.input_cursor = prev;
        }
    }

    fn move_right(&mut self) {
        if let Some(next) = self.next_cursor() {
            self.input_cursor = next;
        }
    }

    fn move_home(&mut self) {
        self.input_cursor = 0;
    }

    fn move_end(&mut self) {
        self.input_cursor = self.input.len();
    }

    fn clear_before_cursor(&mut self) {
        self.input.drain(..self.input_cursor);
        self.input_cursor = 0;
    }

    fn take_input(&mut self) -> String {
        let input = self.input.trim().to_owned();
        self.input.clear();
        self.input_cursor = 0;
        input
    }

    fn prev_cursor(&self) -> Option<usize> {
        self.input[..self.input_cursor]
            .char_indices()
            .last()
            .map(|(index, _)| index)
    }

    fn next_cursor(&self) -> Option<usize> {
        self.input[self.input_cursor..]
            .chars()
            .next()
            .map(|ch| self.input_cursor + ch.len_utf8())
    }
}

struct TerminalGuard {
    terminal: Terminal<CrosstermBackend<Stdout>>,
}

impl TerminalGuard {
    fn enter() -> io::Result<Self> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen)?;
        let mut terminal = Terminal::new(CrosstermBackend::new(stdout))?;
        terminal.show_cursor()?;
        Ok(Self { terminal })
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(self.terminal.backend_mut(), LeaveAlternateScreen);
        let _ = self.terminal.show_cursor();
    }
}

#[derive(Clone)]
struct TuiLogMakeWriter {
    tx: UnboundedSender<String>,
}

struct TuiLogWriter {
    tx: UnboundedSender<String>,
    buffer: Vec<u8>,
}

impl<'a> MakeWriter<'a> for TuiLogMakeWriter {
    type Writer = TuiLogWriter;

    fn make_writer(&'a self) -> Self::Writer {
        TuiLogWriter {
            tx: self.tx.clone(),
            buffer: Vec::new(),
        }
    }
}

impl TuiLogWriter {
    fn emit_complete_lines(&mut self) {
        while let Some(pos) = self.buffer.iter().position(|byte| *byte == b'\n') {
            let line = self.buffer.drain(..=pos).collect::<Vec<_>>();
            self.emit_line(&line);
        }
    }

    fn emit_remainder(&mut self) {
        if self.buffer.is_empty() {
            return;
        }
        let line = std::mem::take(&mut self.buffer);
        self.emit_line(&line);
    }

    fn emit_line(&self, line: &[u8]) {
        let line = String::from_utf8_lossy(line);
        let line = line.trim_end_matches(['\r', '\n']);
        if !line.is_empty() {
            let _ = self.tx.send(line.to_owned());
        }
    }
}

impl Write for TuiLogWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        self.emit_complete_lines();
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.emit_remainder();
        Ok(())
    }
}

impl Drop for TuiLogWriter {
    fn drop(&mut self) {
        self.emit_remainder();
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let (log_tx, log_rx) = mpsc::unbounded_channel();
    let _ = tracing_subscriber::fmt()
        .compact()
        .with_ansi(false)
        .with_writer(TuiLogMakeWriter { tx: log_tx.clone() })
        .try_init();

    let args = parse_args()?;
    let io = Arc::new(LocalhostTcpIo::bind_ephemeral().await?);
    let local_address = io.address;

    let leader_address = args.leader.unwrap_or(local_address);
    let shared = SharedState::start(SharedStateConfig {
        io,
        my_address: local_address,
        leader_address,
        available_peers: vec![leader_address],
        initial_state: RecoverableState::new(local_address as u64, KvStore::new()),
        settings: SharedStateSettings::default(),
    })
    .map_err(|error| Error::other(format!("failed to start shared state: {error:?}")))?;

    let _ = log_tx.send(format!("listening on 127.0.0.1:{local_address} leader=127.0.0.1:{}", leader_address));
    let _ = log_tx.send(COMMAND_HELP.to_owned());

    let mut state_handle = shared.state_handle();
    run_tui(&shared, log_rx, &mut state_handle).await
}

fn parse_args() -> io::Result<Args> {
    let mut args = env::args().skip(1).collect::<Vec<_>>();
    if 1 < args.len() {
        return Err(usage_error());
    }

    let leader = if let Some(raw) = args.pop() {
        Some(raw.parse::<u16>().map_err(|_| usage_error())?)
    } else {
        None
    };

    Ok(Args { leader })
}

fn usage_error() -> io::Error {
    Error::new(ErrorKind::InvalidInput, "usage: cargo run --example kv_tui -- [leader_port]")
}

async fn run_tui(
    shared: &KvShared,
    mut log_rx: UnboundedReceiver<String>,
    state_handle: &mut StateHandle<KvStore>,
) -> io::Result<()> {
    let mut terminal = TerminalGuard::enter()?;
    let mut app = App::new();
    let mut last_render = Instant::now();

    loop {
        while let Ok(message) = log_rx.try_recv() {
            app.log(message);
        }

        if last_render.elapsed() >= RENDER_INTERVAL {
            let summary = build_summary(shared.node(), state_handle).await;
            terminal.terminal.draw(|frame| render(frame, &app, summary.clone()))?;
            last_render = Instant::now();
        }

        if event::poll(Duration::from_millis(20))? {
            if let Event::Key(key) = event::read()? {
                handle_key(key, &mut app, shared, state_handle).await;
            }
        }

        if app.should_quit {
            break;
        }
    }

    Ok(())
}

fn render(frame: &mut ratatui::Frame<'_>, app: &App, summary: Vec<String>) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(frame.area());

    let summary_lines = summary.into_iter().map(Line::from).collect::<Vec<_>>();
    frame.render_widget(
        Paragraph::new(summary_lines)
            .block(Block::default().title("Node").borders(Borders::ALL))
            .wrap(Wrap { trim: false }),
        chunks[0],
    );

    let shell_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(1), Constraint::Length(3)])
        .split(chunks[1]);
    let log_items = app
        .logs
        .iter()
        .rev()
        .take(shell_chunks[0].height.saturating_sub(2) as usize)
        .rev()
        .map(|line| ListItem::new(Line::from(line.clone())))
        .collect::<Vec<_>>();
    frame.render_widget(
        List::new(log_items).block(Block::default().title("Shell").borders(Borders::ALL)),
        shell_chunks[0],
    );

    let inner_width = shell_chunks[1].width.saturating_sub(2);
    let prompt_width = PROMPT.width() as u16;
    let input_width = inner_width.saturating_sub(prompt_width);
    let (visible_input, cursor_col) = input_view(&app.input, app.input_cursor, input_width);
    let prompt = Line::from(vec![
        Span::styled(PROMPT, Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
        Span::raw(visible_input),
    ]);
    frame.render_widget(
        Paragraph::new(prompt).block(Block::default().title("Input (focused)").borders(Borders::ALL)),
        shell_chunks[1],
    );

    if shell_chunks[1].height >= 3 && inner_width > 0 {
        let prompt_col = prompt_width.min(inner_width.saturating_sub(1));
        let max_x = shell_chunks[1].x + shell_chunks[1].width.saturating_sub(2);
        let cursor_x = (shell_chunks[1].x + 1 + prompt_col + cursor_col).min(max_x);
        let cursor_y = shell_chunks[1].y + 1;
        frame.set_cursor_position(Position::new(cursor_x, cursor_y));
    }
}

fn input_view(input: &str, cursor: usize, max_width: u16) -> (String, u16) {
    let max_width = max_width as usize;
    if max_width == 0 {
        return (String::new(), 0);
    }

    let max_cursor_col = max_width.saturating_sub(1);
    let mut start = cursor;
    let mut cursor_width = 0usize;
    for (index, ch) in input[..cursor].char_indices().rev() {
        let width = ch.width().unwrap_or(0);
        if cursor_width + width > max_cursor_col {
            break;
        }
        cursor_width += width;
        start = index;
    }

    let mut end = input.len();
    let mut visible_width = 0usize;
    for (offset, ch) in input[start..].char_indices() {
        let width = ch.width().unwrap_or(0);
        if visible_width + width > max_width {
            end = start + offset;
            break;
        }
        visible_width += width;
    }

    let cursor_col = input[start..cursor].width().min(max_cursor_col) as u16;
    (input[start..end].to_owned(), cursor_col)
}

async fn build_summary(state: &NodeState<u16, KvStore>, state_handle: &mut StateHandle<KvStore>) -> Vec<String> {
    let (seq, item_count, values_preview) = state_handle.read_with(|recoverable| {
        let store = recoverable.state();
        let preview = store
            .values
            .iter()
            .take(6)
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>();
        (store.seq, store.values.len(), preview)
    });

    let mut lines = vec![
        format!("address: 127.0.0.1:{}", state.my_address),
        format!("leader: 127.0.0.1:{} ({})", state.leader_address(), if state.is_leader() { "self" } else { "remote" }),
        format!(
            "leader connection: {}",
            if state.is_connected_to_leader() {
                "connected"
            } else {
                "not connected"
            }
        ),
        format!("kv: seq={seq} items={item_count}"),
    ];
    if !values_preview.is_empty() {
        lines.push(format!("kv preview: {}", values_preview.join(", ")));
    }

    lines
}

async fn handle_key(key: KeyEvent, app: &mut App, shared: &KvShared, state_handle: &mut StateHandle<KvStore>) {
    match key.code {
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.should_quit = true;
        }
        KeyCode::Char('a') if key.modifiers.contains(KeyModifiers::CONTROL) => app.move_home(),
        KeyCode::Char('e') if key.modifiers.contains(KeyModifiers::CONTROL) => app.move_end(),
        KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => app.clear_before_cursor(),
        KeyCode::Esc => app.should_quit = true,
        KeyCode::Left => app.move_left(),
        KeyCode::Right => app.move_right(),
        KeyCode::Home => app.move_home(),
        KeyCode::End => app.move_end(),
        KeyCode::Backspace => app.backspace(),
        KeyCode::Delete => app.delete(),
        KeyCode::Enter => {
            let command = app.take_input();
            if !command.is_empty() {
                app.log(format!("kv> {command}"));
                run_command(command, app, shared, state_handle).await;
            }
        }
        KeyCode::Char(ch) => {
            if !key.modifiers.intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) {
                app.insert_char(ch);
            }
        }
        _ => {}
    }
}

async fn run_command(command: String, app: &mut App, shared: &KvShared, state_handle: &mut StateHandle<KvStore>) {
    let mut parts = command.splitn(2, char::is_whitespace);
    let name = parts.next().unwrap_or_default();
    let rest = parts.next().unwrap_or_default().trim();

    match name {
        "help" => app.log(COMMAND_HELP),
        "quit" | "exit" => app.should_quit = true,
        "status" => {
            app.log(format!(
                "address={} leader={} mode={} connected_to_leader={}",
                shared.my_address(),
                shared.leader_address(),
                if shared.is_leader() { "leader" } else { "follower" },
                shared.is_connected_to_leader()
            ));
        }
        "get" => handle_get(rest, app, state_handle),
        "set" => handle_set(rest, app, shared).await,
        "delete" | "del" | "rm" => handle_delete(rest, app, shared).await,
        "list" | "print" => handle_list(app, state_handle),
        _ => app.log(format!("unknown command: {name}")),
    }
}

fn handle_get(rest: &str, app: &mut App, state_handle: &mut StateHandle<KvStore>) {
    let key = rest.trim();
    if key.is_empty() || key.split_whitespace().nth(1).is_some() {
        app.log("usage: get <key>");
        return;
    }

    let value = state_handle.read_with(|state| state.state().values.get(key).cloned());
    match value {
        Some(value) => app.log(format!("{key}={value}")),
        None => app.log(format!("{key}=(missing)")),
    }
}

async fn handle_set(rest: &str, app: &mut App, shared: &KvShared) {
    let mut parts = rest.splitn(2, char::is_whitespace);
    let Some(key) = parts.next().filter(|value| !value.is_empty()) else {
        app.log("usage: set <key> <value>");
        return;
    };
    let Some(value) = parts.next().map(str::trim).filter(|value| !value.is_empty()) else {
        app.log("usage: set <key> <value>");
        return;
    };

    match shared
        .submit_action(KvAction::Set {
            key: key.to_owned(),
            value: value.to_owned(),
        })
        .await
    {
        Ok(()) => app.log(format!("queued set {key}")),
        Err(error) => app.log(format!("failed to queue set: {error}")),
    }
}

async fn handle_delete(rest: &str, app: &mut App, shared: &KvShared) {
    let key = rest.trim();
    if key.is_empty() || key.split_whitespace().nth(1).is_some() {
        app.log("usage: delete <key>");
        return;
    }

    match shared.submit_action(KvAction::Delete { key: key.to_owned() }).await {
        Ok(()) => app.log(format!("queued delete {key}")),
        Err(error) => app.log(format!("failed to queue delete: {error}")),
    }
}

fn handle_list(app: &mut App, state_handle: &mut StateHandle<KvStore>) {
    let values = state_handle.read_with(|state| state.state().values.clone());
    if values.is_empty() {
        app.log("(empty)");
        return;
    }

    for (key, value) in values {
        app.log(format!("{key}={value}"));
    }
}
