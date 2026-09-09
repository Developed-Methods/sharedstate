use super::ReplicatedState;
use crate::state::deterministic_state::DeterministicState;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

/// Accept already-converted deterministic updates from a legacy state machine.
/// Convert client actions statelessly before submission; this adapter never calls `authority`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LegacyState<D, const SCHEMA: u32> {
    pub state: D,
}

impl<D, const SCHEMA: u32> ReplicatedState for LegacyState<D, SCHEMA>
where
    D: DeterministicState + Serialize + DeserializeOwned + std::fmt::Debug,
    D::AuthorityAction: Serialize + DeserializeOwned,
{
    type Command = D::AuthorityAction;
    type Result = ();
    const SCHEMA_VERSION: u32 = SCHEMA;
    fn apply(&mut self, command: Self::Command) {
        self.state.update(&command);
    }
}
