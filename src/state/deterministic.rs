pub trait DeterministicState: Sized + Send + Sync + Clone + 'static {
    type Action: Sized + Send + Sync + 'static;
    type AuthorityAction: Sized + Clone + Send + Sync + 'static;

    fn accept_seq(&self) -> u64;

    /// Converts a client action into an authority action.
    ///
    /// This method must not rely on observing effects from recently returned
    /// authority actions. Leaders may enqueue authority actions before the
    /// local hot-read snapshot has applied earlier actions, and forwarded
    /// actions may be authorized concurrently. Use this for stateless wrapping
    /// or validation of client context; state-dependent sequencing should
    /// happen in `update`.
    fn authority(&self, action: Self::Action) -> Self::AuthorityAction;

    fn update(&mut self, action: &Self::AuthorityAction);
}
