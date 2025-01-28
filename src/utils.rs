use std::{fmt::Debug, panic::Location, time::Duration, future::Future};

pub trait LogHelper {
    fn log(self) -> Self;

    fn err_log(self, msg: &str) -> Self;
}

impl<R, E: Debug> LogHelper for Result<R, E> {
    #[track_caller]
    fn log(self) -> Self {
        let msg = std::any::type_name::<E>().rsplit("::").next().unwrap();
        match self {
            Err(error) => {
                let caller = Location::caller();
                tracing::error!(?error, "Error({}:{}), {} ", caller.file(), caller.line(), msg);
                Err(error)
            }
            ok => ok,
        }
    }

    #[track_caller]
    fn err_log(self, msg: &str) -> Self {
        match self {
            Err(error) => {
                let caller = Location::caller();
                tracing::error!(?error, "Error({}:{}), {} ", caller.file(), caller.line(), msg);
                Err(error)
            }
            ok => ok,
        }
    }
}

impl<R> LogHelper for Option<R> {
    #[track_caller]
    fn log(self) -> Self {
        let name = std::any::type_name::<R>().rsplit("::").next().unwrap();
        self.err_log(name)
    }

    #[track_caller]
    fn err_log(self, msg: &str) -> Self {
        match self {
            None => {
                let caller = Location::caller();
                tracing::error!("GotNone({}:{}), {} ", caller.file(), caller.line(), msg);
                None
            }
            ok => ok,
        }
    }
}

pub trait PanicHelper {
    type Success;

    fn panic(self, msg: &str) -> Self::Success;
}

impl<R, E> PanicHelper for Result<R, E> {
    type Success = R;

    #[track_caller]
    fn panic(self, msg: &str) -> R {
        match self {
            Ok(v) => v,
            Err(_) => {
                let caller = Location::caller();
                tracing::error!("Panic({}:{}), {} ", caller.file(), caller.line(), msg);
                panic!("Panic({}:{}), {} ", caller.file(), caller.line(), msg);
            }
        }
    }
}

impl<R> PanicHelper for Option<R> {
    type Success = R;

    #[track_caller]
    fn panic(self, msg: &str) -> R {
        match self {
            Some(v) => v,
            None => {
                let caller = Location::caller();
                tracing::error!("Panic({}:{}), {} ", caller.file(), caller.line(), msg);
                panic!("Panic({}:{}), {} ", caller.file(), caller.line(), msg);
            }
        }
    }
}

pub trait TimeoutPanicHelper: Sized {
    type Success;

    #[track_caller]
    fn timeout(self, time: Duration, msg: &str) -> impl Future<Output = Self::Success> {
        let caller = Location::caller();

        async move {
            match self._timeout(caller, time, msg).await {
                Ok(v) => v,
                Err(msg) => {
                    tracing::error!("{}", msg);
                    panic!("{}", msg);
                }
            }
        }
    }

    fn _timeout(self, caller: &'static Location<'static>, time: Duration, msg: &str) -> impl Future<Output = Result<Self::Success, String>>;

    #[cfg(test)]
    #[track_caller]
    fn test_timeout(self) -> impl Future<Output = Self::Success> {
        let caller = Location::caller();

        async move {
            match self._timeout(caller, Duration::from_millis(100), "test").await {
                Ok(v) => v,
                Err(msg) => {
                    tracing::error!("{}", msg);
                    panic!("{}", msg);
                }
            }
        }
    }

    #[cfg(test)]
    #[track_caller]
    fn expect_timeout(self) -> impl Future<Output = ()> {
        let caller = Location::caller();

        async move {
            if self._timeout(caller, Duration::from_millis(100), "").await.is_ok() {
                let msg = format!("did not get expected timeout at 100ms, {}:{}", caller.file(), caller.line());
                tracing::error!("{}", msg);
                panic!("{}", msg);
            }
        }
    }
}

impl<F: Future> TimeoutPanicHelper for F {
    type Success = F::Output;

    async fn _timeout(self, caller: &'static Location<'static>, time: Duration, msg: &str) -> Result<Self::Success, String> {
        match tokio::time::timeout(time, self).await {
            Ok(r) => Ok(r),
            Err(_) => Err(format!("Timeout({:?}) Panic({}:{}), {} ", time, caller.file(), caller.line(), msg)),
        }
    }
}

