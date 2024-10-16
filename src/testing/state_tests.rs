use std::{sync::{atomic::{AtomicBool, Ordering}, Arc}, time::{Duration, Instant}};

use message_encoding::MessageEncoding;

use crate::{state::{DeterministicState, SharedState}, message_io::unknown_id_err};

#[derive(Clone, Debug, Default)]
pub struct TestState {
    pub sequence: u64,
    pub numbers: [i64; 6],
}

impl DeterministicState for TestState {
    type Action = TestStateAction;

    fn id(&self) -> u64 {
        1
    }

    fn sequence(&self) -> u64 {
        self.sequence
    }

    fn update(&mut self, action: &Self::Action) {
        self.sequence += 1;

        match action {
            TestStateAction::Add { slot, value } => self.numbers[*slot] += *value,
            TestStateAction::Set { slot, value } => self.numbers[*slot] = *value,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TestStateAction {
    Add { slot: usize, value: i64 },
    Set { slot: usize, value: i64 },
}

impl MessageEncoding for TestState {
    fn write_to<T: std::io::prelude::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        let mut sum = 0;
        sum += self.sequence.write_to(out)?;
        for num in self.numbers {
            sum += (num as u64).write_to(out)?;
        }
        Ok(sum)
    }

    fn read_from<T: std::io::prelude::Read>(read: &mut T) -> std::io::Result<Self> {
        Ok(TestState {
            sequence: MessageEncoding::read_from(read)?,
            numbers: [
                u64::read_from(read)? as i64,
                u64::read_from(read)? as i64,
                u64::read_from(read)? as i64,
                u64::read_from(read)? as i64,
                u64::read_from(read)? as i64,
                u64::read_from(read)? as i64,
            ],
        })
    }
}

impl MessageEncoding for TestStateAction {
    fn write_to<T: std::io::prelude::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        let mut sum = 0;
        match self {
            Self::Add { slot, value } => {
                sum += 1u16.write_to(out)?;
                sum += (*slot as u64).write_to(out)?;
                sum += (*value as u64).write_to(out)?;
            }
            Self::Set { slot, value } => {
                sum += 2u16.write_to(out)?;
                sum += (*slot as u64).write_to(out)?;
                sum += (*value as u64).write_to(out)?;
            }
        }
        Ok(sum)
    }

    fn read_from<T: std::io::prelude::Read>(read: &mut T) -> std::io::Result<Self> {
        match u16::read_from(read)? {
            1 => Ok(Self::Add {
                slot: u64::read_from(read)? as usize,
                value: u64::read_from(read)? as i64,
            }),
            2 => Ok(Self::Set {
                slot: u64::read_from(read)? as usize,
                value: u64::read_from(read)? as i64,
            }),
            other => Err(unknown_id_err(other, "TestStateAction")),
        }
    }
}

#[test]
fn slow_state_deadlock_test() {
    super::setup_logging();

    let (state, mut updater) = SharedState::new(TestState { sequence: 0, numbers: [0i64; 6] });

    let run = Arc::new(AtomicBool::new(true));
    let failed = Arc::new(AtomicBool::new(false));
    let thread_count = 5;

    let mut reader_threads = Vec::with_capacity(thread_count);
    for _ in 0..thread_count {
        let thread = std::thread::spawn({
            let run = run.clone();
            let failed = failed.clone();
            let state = state.clone();

            move || {
                let mut update = Instant::now();
                let mut last_sequence = 0;

                while run.load(Ordering::Acquire) {
                    let lock = state.read();
                    let reader = &*lock;

                    if reader.sequence != last_sequence {
                        last_sequence = reader.sequence;
                        update = Instant::now();
                        continue;
                    }

                    if update.elapsed() > Duration::from_millis(100) {
                        failed.store(true, Ordering::SeqCst);
                        break;
                    }
                }
            }
        });

        reader_threads.push(thread);
    }

    let start = Instant::now();
    let mut last_update = Instant::now();

    while start.elapsed() < Duration::from_secs(10) {
        std::thread::yield_now();
        if updater.update_ready() {
            updater.update();
        }

        let now = Instant::now();
        let elapsed = now - last_update;
        if Duration::from_millis(50) < elapsed  {
            updater.queue(TestStateAction::Add { slot: 3, value: 1 });
            last_update = now;
        }
    }

    run.store(false, Ordering::Release);

    for thread in reader_threads {
        thread.join().unwrap();
    }

    assert!(!failed.load(Ordering::SeqCst));
    println!("{:?}", state.read());
}