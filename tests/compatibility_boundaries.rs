use std::io;

use message_encoding::MessageEncoding;
use sharedstate::{
    protocol::messages::{SyncRequest, SyncResponse},
    state::{
        deterministic_state::DeterministicState,
        recoverable_state::{RecoverableState, RecoverableStateAction, RecoverableStateDetails},
    },
};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct CounterState(u64);

impl DeterministicState for CounterState {
    type Action = u64;
    type AuthorityAction = u64;

    fn accept_seq(&self) -> u64 {
        self.0
    }

    fn authority(&self, action: Self::Action) -> Self::AuthorityAction {
        action
    }

    fn update(&mut self, _action: &Self::AuthorityAction) {
        self.0 += 1;
    }
}

impl MessageEncoding for CounterState {
    const STATIC_SIZE: Option<usize> = u64::STATIC_SIZE;
    const MAX_SIZE: Option<usize> = u64::MAX_SIZE;

    fn write_to<T: io::Write>(&self, out: &mut T) -> io::Result<usize> {
        self.0.write_to(out)
    }

    fn read_from<T: io::Read>(read: &mut T) -> io::Result<Self> {
        Ok(Self(MessageEncoding::read_from(read)?))
    }
}

fn encoded<M: MessageEncoding>(message: &M) -> Vec<u8> {
    let mut bytes = Vec::new();
    let written = message.write_to(&mut bytes).unwrap();
    assert_eq!(written, bytes.len());
    bytes
}

fn sample_details() -> RecoverableStateDetails {
    let mut details = RecoverableStateDetails::new(0x0102_0304_0506_0708, 0x1112_1314_1516_1718);
    details.apply_generation_bump(0x2122_2324_2526_2728);
    details
}

#[test]
fn generation_history_retains_exactly_2048_boundaries() {
    let mut leader = RecoverableState::new(1000, CounterState::default());

    let just_beyond_retention = leader.details().clone();

    leader.update(&RecoverableStateAction::BumpGeneration { new_id: 1001 });
    let oldest_retained = leader.details().clone();

    for new_id in 1002..=3049 {
        leader.update(&RecoverableStateAction::BumpGeneration { new_id });
    }

    assert!(leader.details().can_recover_follower(&oldest_retained));
    assert!(!leader.details().can_recover_follower(&just_beyond_retention));
}

#[test]
fn recoverable_state_details_wire_snapshot() {
    let details = sample_details();

    assert_eq!(
        encoded(&details),
        vec![
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // outer_state_next_seq
            0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, // id
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // generation
            0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, // inner_state_next_seq
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // history length
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // history[0].old_id
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // history[0].generation
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // history[0].next_sequence
        ]
    );
}

#[test]
fn sync_request_subscribe_wire_snapshot() {
    let request = SyncRequest::<CounterState>::Subscribe(sample_details());

    assert_eq!(
        encoded(&request),
        vec![
            0x00, 0x01, // SyncRequest::Subscribe
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // outer_state_next_seq
            0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, // id
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // generation
            0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, // inner_state_next_seq
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // history length
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // history[0].old_id
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // history[0].generation
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // history[0].next_sequence
        ]
    );
}

#[test]
fn sync_response_action_wire_snapshot() {
    let response = SyncResponse::<CounterState>::Action {
        seq: 0x3132_3334_3536_3738,
        action: RecoverableStateAction::BumpGeneration {
            new_id: 0x4142_4344_4546_4748,
        },
    };

    assert_eq!(
        encoded(&response),
        vec![
            0x00, 0x03, // SyncResponse::Action
            0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, // seq
            0x00, 0x02, // RecoverableStateAction::BumpGeneration
            0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, // new_id
        ]
    );
}
