use std::time::{SystemTime, UNIX_EPOCH};

use message_encoding::MessageEncoding;
use serde::{Deserialize, Serialize};
use sharedstate::{net::message_io::unknown_id_err, state::DeterministicState};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebTestState {
    pub sequence: u64,
    pub counters: [i64; 8],
    pub log: Vec<WebTestLogEntry>,
}

impl Default for WebTestState {
    fn default() -> Self {
        Self {
            sequence: 0,
            counters: [0; 8],
            log: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebTestLogEntry {
    pub sequence: u64,
    pub millis: u64,
    pub label: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum WebTestAction {
    Add { slot: usize, value: i64 },
    Set { slot: usize, value: i64 },
    AppendLog { label: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WebTestAuthorityAction {
    millis: u64,
    action: WebTestAction,
}

impl DeterministicState for WebTestState {
    type Action = WebTestAction;
    type AuthorityAction = WebTestAuthorityAction;

    fn accept_seq(&self) -> u64 {
        self.sequence
    }

    fn authority(&self, action: Self::Action) -> Self::AuthorityAction {
        WebTestAuthorityAction {
            millis: now_ms(),
            action,
        }
    }

    fn update(&mut self, action: &Self::AuthorityAction) {
        self.sequence += 1;

        match &action.action {
            WebTestAction::Add { slot, value } => {
                if let Some(counter) = self.counters.get_mut(*slot) {
                    *counter = counter.overflowing_add(*value).0;
                }
            }
            WebTestAction::Set { slot, value } => {
                if let Some(counter) = self.counters.get_mut(*slot) {
                    *counter = *value;
                }
            }
            WebTestAction::AppendLog { label } => {
                self.log.push(WebTestLogEntry {
                    sequence: self.sequence,
                    millis: action.millis,
                    label: label.clone(),
                });
                if 100 < self.log.len() {
                    let excess = self.log.len() - 100;
                    self.log.drain(0..excess);
                }
            }
        }
    }
}

impl MessageEncoding for WebTestState {
    fn write_to<T: std::io::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        let mut sum = 0;
        sum += self.sequence.write_to(out)?;
        for counter in self.counters {
            sum += (counter as u64).write_to(out)?;
        }
        sum += (self.log.len() as u64).write_to(out)?;
        for entry in &self.log {
            sum += entry.sequence.write_to(out)?;
            sum += entry.millis.write_to(out)?;
            sum += entry.label.write_to(out)?;
        }
        Ok(sum)
    }

    fn read_from<T: std::io::Read>(read: &mut T) -> std::io::Result<Self> {
        let sequence = MessageEncoding::read_from(read)?;
        let counters = [
            u64::read_from(read)? as i64,
            u64::read_from(read)? as i64,
            u64::read_from(read)? as i64,
            u64::read_from(read)? as i64,
            u64::read_from(read)? as i64,
            u64::read_from(read)? as i64,
            u64::read_from(read)? as i64,
            u64::read_from(read)? as i64,
        ];
        let len = u64::read_from(read)? as usize;
        let mut log = Vec::with_capacity(len.min(100));
        for _ in 0..len {
            log.push(WebTestLogEntry {
                sequence: MessageEncoding::read_from(read)?,
                millis: MessageEncoding::read_from(read)?,
                label: MessageEncoding::read_from(read)?,
            });
        }

        Ok(Self {
            sequence,
            counters,
            log,
        })
    }
}

impl MessageEncoding for WebTestAction {
    fn write_to<T: std::io::Write>(&self, out: &mut T) -> std::io::Result<usize> {
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
            Self::AppendLog { label } => {
                sum += 3u16.write_to(out)?;
                sum += label.write_to(out)?;
            }
        }
        Ok(sum)
    }

    fn read_from<T: std::io::Read>(read: &mut T) -> std::io::Result<Self> {
        match u16::read_from(read)? {
            1 => Ok(Self::Add {
                slot: u64::read_from(read)? as usize,
                value: u64::read_from(read)? as i64,
            }),
            2 => Ok(Self::Set {
                slot: u64::read_from(read)? as usize,
                value: u64::read_from(read)? as i64,
            }),
            3 => Ok(Self::AppendLog {
                label: MessageEncoding::read_from(read)?,
            }),
            other => Err(unknown_id_err(other, "WebTestAction")),
        }
    }
}

impl MessageEncoding for WebTestAuthorityAction {
    fn write_to<T: std::io::Write>(&self, out: &mut T) -> std::io::Result<usize> {
        Ok(self.millis.write_to(out)? + self.action.write_to(out)?)
    }

    fn read_from<T: std::io::Read>(read: &mut T) -> std::io::Result<Self> {
        Ok(Self {
            millis: MessageEncoding::read_from(read)?,
            action: MessageEncoding::read_from(read)?,
        })
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
