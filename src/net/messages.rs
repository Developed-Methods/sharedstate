use crate::recoverable_state::RecoverableStateDetails;

pub enum SyncRequests {
    RequestRecoveryHeader,
    SubscribeFresh,
    SubscribeRecovery(RecoverableStateDetails),
}

pub enum SyncResponses {
    RecoverState(RecoverableStateDetails),
    PrepareForStateAndFeed,
    PrepareForFeed,
}
