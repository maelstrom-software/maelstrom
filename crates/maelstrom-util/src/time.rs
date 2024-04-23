use std::cell::Cell;
use std::time::{Duration, Instant};

pub trait ClockInstant {
    fn elapsed(&self) -> Duration;
}

pub trait Clock {
    type Instant<'a>: ClockInstant
    where
        Self: 'a;

    fn now(&self) -> Self::Instant<'_>;
}

pub struct SystemMonotonicClock;

impl Clock for SystemMonotonicClock {
    type Instant<'a> = Instant;

    fn now(&self) -> Instant {
        Instant::now()
    }
}

impl ClockInstant for Instant {
    fn elapsed(&self) -> Duration {
        Instant::elapsed(self)
    }
}

pub struct SecondsInstant<'a, ClockT> {
    value: u64,
    clock: &'a ClockT,
}

impl<'a, ClockT> ClockInstant for SecondsInstant<'a, ClockT>
where
    ClockT: Clock<Instant<'a> = Self>,
{
    fn elapsed(&self) -> Duration {
        let now = self.clock.now();
        Duration::from_secs(now.value - self.value)
    }
}

#[derive(Default)]
pub struct TickingClock(Cell<u64>);

impl TickingClock {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Clock for TickingClock {
    type Instant<'a> = SecondsInstant<'a, Self>;

    fn now(&self) -> SecondsInstant<'_, Self> {
        let current_value = self.0.get();
        self.0.set(current_value + 1);
        SecondsInstant {
            value: current_value,
            clock: self,
        }
    }
}
