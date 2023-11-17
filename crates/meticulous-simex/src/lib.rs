use std::{collections::VecDeque, iter::Fuse};
use num::integer::{Average, Integer};

#[derive(Default)]
struct SimulationState {
    script: Vec<bool>,
    cursor: usize,
}

impl SimulationState {
    fn choose_bool(&mut self) -> bool {
        let result = if self.cursor == self.script.len() {
            self.script.push(false);
            false
        } else {
            self.script[self.cursor]
        };
        self.cursor += 1;
        result
    }

    fn next_simulation(&mut self) -> bool {
        assert!(self.cursor == self.script.len());
        self.cursor = 0;
        while let Some(last) = self.script.last_mut() {
            if *last {
                self.script.pop();
            } else {
                *last = true;
                return true;
            }
        }
        false
    }
}

pub struct Map<'a, F> {
    simex: &'a mut SimulationExplorer,
    f: F,
}

impl<'a, F, T> Iterator for Map<'a, F>
where
    F: for<'b> FnMut(Simulation<'b>) -> T,
{
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        self.simex.next_simulation().map(|sim| (self.f)(sim))
    }
}

pub struct ChooseN<'a, 'b, T> {
    simulation: &'b mut Simulation<'a>,
    n: usize,
    iter: T,
}

impl<'a, 'b, T> Iterator for ChooseN<'a, 'b, T>
where
    T: ExactSizeIterator,
{
    type Item = T::Item;
    fn next(&mut self) -> Option<Self::Item> {
        match (self.n, self.iter.len()) {
            (0, _) | (_, 0) => None,
            (n, remaining) if n >= remaining => {
                self.n = n - 1;
                self.iter.next()
            }
            (n, remaining) => {
                let extra = remaining - n;
                self.n = n - 1;
                self.iter.nth(self.simulation.choose_integer_unchecked(0, extra))
            }
        }
    }
}

pub struct ChooseNUnknownSize<'a, 'b, T>
where
    T: Iterator,
{
    simulation: &'b mut Simulation<'a>,
    reservoir: VecDeque<T::Item>,
    n: usize,
    iter: Fuse<T>,
}

impl<'a, 'b, T> Iterator for ChooseNUnknownSize<'a, 'b, T>
where
    T: Iterator,
{
    type Item = T::Item;
    fn next(&mut self) -> Option<Self::Item> {
        if self.n == 0 {
            return None;
        }
        loop {
            // Fill the reservoir.
            while self.reservoir.len() < self.n + 1 {
                match self.iter.next() {
                    Some(elem) => self.reservoir.push_back(elem),
                    None => {
                        // At this point, we no longer care about n. The iterator is fused, so
                        // we're always going to take this same code path upon subsequent calls to
                        // this method.
                        return self.reservoir.pop_front();
                    }
                }
            }
            // Take the front element from the reservoir and either return it or drop it.
            let front = self.reservoir.pop_front();
            if !self.simulation.choose_bool() {
                self.n -= 1;
                return front;
            }
        }
    }
}

#[derive(Default)]
pub struct SimulationExplorer {
    state: Option<SimulationState>,
}

impl SimulationExplorer {
    pub fn next_simulation(&mut self) -> Option<Simulation<'_>> {
        match self.state {
            None => {
                self.state = Some(SimulationState::default());
                Some(Simulation {
                    state: self.state.as_mut().unwrap(),
                })
            }
            Some(ref mut state) => {
                if state.next_simulation() {
                    Some(Simulation { state })
                } else {
                    None
                }
            }
        }
    }

    pub fn map<F, T>(&mut self, f: F) -> Map<'_, F>
    where
        F: for<'b> FnMut(Simulation<'b>) -> T,
    {
        Map { simex: self, f }
    }
}

pub struct Simulation<'a> {
    state: &'a mut SimulationState,
}

impl<'a> Simulation<'a> {
    /// Choose a boolean value. Values will be returned in ascending order: `false` then `true`.
    ///
    /// This is the primitive upon which all other `choose_` functions are built.
    ///
    /// This function has constant time complexity, and increases the size of the `Simulation` by
    /// small, constant amount.
    ///
    /// # Examples
    /// ```
    /// use meticulous_simex::SimulationExplorer;
    /// let mut simex = SimulationExplorer::default();
    /// assert!(simex.map(|mut sim| sim.choose_bool()).eq([false, true]))
    /// ```
    pub fn choose_bool(&mut self) -> bool {
        self.state.choose_bool()
    }

    /// Choose an integer value in the closed range `[min, max]`. Values will be returned in
    /// ascending order.
    ///
    /// This function has time complexity of O(log(max-min)). It also increases the size of the
    /// `Simulation` by small O(log(max-min)) amount.
    ///
    /// # Panics
    /// This function will panic if `max` is less than `min`.
    ///
    /// # Examples
    /// ```
    /// use meticulous_simex::SimulationExplorer;
    /// assert!(SimulationExplorer::default().map(|mut sim| sim.choose_integer(0, 10)).eq(0..=10));
    /// assert!(SimulationExplorer::default().map(|mut sim| sim.choose_integer(-1, 1)).eq(-1..=1));
    /// ```
    pub fn choose_integer<T: Average + Copy + Integer>(&mut self, min: T, max: T) -> T {
        assert!(min <= max);
        self.choose_integer_unchecked(min, max)
    }

    /// Like `choose_integer`, but no check is done to asser that `min <= max`. If `min > max`,
    /// then the program will likely crash by running out of memory, or suffer some other similar
    /// fate.
    fn choose_integer_unchecked<T: Average + Copy + Integer>(&mut self, min: T, max: T) -> T {
        if min == max {
            min
        } else if !self.choose_bool() {
            self.choose_integer_unchecked(min, min.average_floor(&max))
        } else {
            self.choose_integer_unchecked(min.average_floor(&max) + T::one(), max)
        }
    }

    /// Choose one value out of the given `ExactSizeIterator`. If the iterator is empty, return
    /// `None`.
    ///
    /// Values are returned in the iterator's order.
    ///
    /// If N is `i.len()`, then this function does O(log(N)) work and then calls `Iterator::nth`.
    /// Thus, the time complexity can be as low as O(log(N)), and as high as O(N) (assuming
    /// `Iterator::nth` is no worse than O(N)).
    ///
    /// This function will increase the size of `Simulation` by a small O(log(N)) amount.
    /// # Examples
    /// ```
    /// use meticulous_simex::SimulationExplorer;
    /// assert!(SimulationExplorer::default().map(|mut sim| sim.choose(0..10).unwrap()).eq(0..10));
    /// assert!(SimulationExplorer::default().map(|mut sim| sim.choose([-1, 0, 1]).unwrap()).eq(-1..=1));
    /// ```
    pub fn choose<T>(&mut self, i: T) -> Option<T::Item>
    where
        T: IntoIterator,
        T::IntoIter: ExactSizeIterator,
    {
        let mut iter = i.into_iter();
        match iter.len() {
            0 => None,
            n => iter.nth(self.choose_integer_unchecked(0, n - 1)),
        }
    }

    pub fn choose_n<'b, T>(&'b mut self, n: usize, i: T) -> ChooseN<'a, 'b, T::IntoIter>
    where
        T: IntoIterator,
        T::IntoIter: ExactSizeIterator,
        'a: 'b,
    {
        ChooseN {
            simulation: self,
            n,
            iter: i.into_iter(),
        }
    }

    pub fn choose_unknown_size<T: IntoIterator>(&mut self, i: T) -> Option<T::Item>
    {
        let mut iter = i.into_iter();
        let Some(mut a) = iter.next() else {
            return None;
        };
        for b in iter {
            if self.choose_bool() {
                a = b;
            } else {
                break;
            }
        }
        Some(a)
    }

    pub fn choose_n_unknown_size<'b, T>(
        &'b mut self,
        n: usize,
        i: T,
    ) -> ChooseNUnknownSize<'a, 'b, T::IntoIter>
    where
        T: IntoIterator,
        'a: 'b,
    {
        ChooseNUnknownSize {
            simulation: self,
            reservoir: VecDeque::with_capacity(n + 1),
            n,
            iter: i.into_iter().fuse(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FusedAssertingIterator<T> {
        inner: T,
        has_returned_none: bool,
    }

    impl<T> FusedAssertingIterator<T> {
        fn from(inner: T) -> Self {
            FusedAssertingIterator {
                inner,
                has_returned_none: false,
            }
        }
    }

    impl<T: Iterator> Iterator for FusedAssertingIterator<T> {
        type Item = T::Item;
        fn next(&mut self) -> Option<Self::Item> {
            assert!(!self.has_returned_none);
            let result = self.inner.next();
            if result.is_none() {
                self.has_returned_none = true;
            }
            result
        }
    }

    impl<T: ExactSizeIterator> ExactSizeIterator for FusedAssertingIterator<T> {
        fn len(&self) -> usize {
            self.inner.len()
        }
    }

    #[test]
    fn next_simulation_on_empty_is_false() {
        let mut simex = SimulationExplorer::default();
        assert!(simex.next_simulation().is_some());
        assert!(simex.next_simulation().is_none());
        assert!(simex.next_simulation().is_none());
    }

    #[test]
    fn general_exercise() {
        let mut simex = SimulationExplorer::default();

        let mut simulation = simex.next_simulation().unwrap();
        assert!(!simulation.choose_bool());
        assert!(!simulation.choose_bool());
        assert!(!simulation.choose_bool());
        assert!(!simulation.choose_bool());

        let mut simulation = simex.next_simulation().unwrap();
        assert!(!simulation.choose_bool());
        assert!(!simulation.choose_bool());
        assert!(!simulation.choose_bool());
        assert!(simulation.choose_bool());
        assert!(!simulation.choose_bool());

        let mut simulation = simex.next_simulation().unwrap();
        assert!(!simulation.choose_bool());
        assert!(!simulation.choose_bool());
        assert!(!simulation.choose_bool());
        assert!(simulation.choose_bool());
        assert!(simulation.choose_bool());

        let mut simulation = simex.next_simulation().unwrap();
        assert!(!simulation.choose_bool());
        assert!(!simulation.choose_bool());
        assert!(simulation.choose_bool());

        let mut simulation = simex.next_simulation().unwrap();
        assert!(!simulation.choose_bool());
        assert!(simulation.choose_bool());

        let mut simulation = simex.next_simulation().unwrap();
        assert!(simulation.choose_bool());
        assert!(!simulation.choose_bool());

        let mut simulation = simex.next_simulation().unwrap();
        assert!(simulation.choose_bool());
        assert!(simulation.choose_bool());

        assert!(simex.next_simulation().is_none());
    }

    #[test]
    fn choose_range() {
        let mut simex = SimulationExplorer::default();

        for i in 10..20 {
            let mut simulation = simex.next_simulation().unwrap();
            let j = simulation
                .choose(FusedAssertingIterator::from(10..20))
                .unwrap();
            assert_eq!(i, j);
        }
        assert!(simex.next_simulation().is_none());
    }

    #[test]
    fn choose_vector() {
        let mut simex = SimulationExplorer::default();

        for i in 1..=4 {
            let mut simulation = simex.next_simulation().unwrap();
            let j = simulation.choose(vec![1, 2, 3, 4]).unwrap();
            assert_eq!(i, j);
        }
        assert!(simex.next_simulation().is_none());
    }

    #[test]
    fn choose_empty() {
        let mut simex = SimulationExplorer::default();
        let mut simulation = simex.next_simulation().unwrap();
        assert!(simulation
            .choose(FusedAssertingIterator::from(0..0))
            .is_none());
        assert!(simex.next_simulation().is_none());
    }

    #[test]
    fn while_let() {
        let mut simex = SimulationExplorer::default();
        let mut actual = vec![];
        while let Some(mut simulation) = simex.next_simulation() {
            actual.push(
                simulation
                    .choose(FusedAssertingIterator::from(0..10))
                    .unwrap(),
            );
        }
        assert_eq!(actual, (0..10).collect::<Vec<_>>());
    }

    #[test]
    fn map() {
        assert!(SimulationExplorer::default()
            .map(|mut sim| sim.choose(FusedAssertingIterator::from(0..10)).unwrap())
            .eq(FusedAssertingIterator::from(0..10)));
    }

    #[test]
    fn choose_bool() {
        assert_eq!(
            SimulationExplorer::default()
                .map(|mut sim| sim
                    .choose_bool())
                    .collect::<Vec<_>>(),
                vec![false, true],
        );
    }

    #[test]
    fn choose_n() {
        assert_eq!(
            SimulationExplorer::default()
                .map(|mut sim| sim
                    .choose_n(3, FusedAssertingIterator::from(0..5))
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            vec![
                vec![0, 1, 2],
                vec![0, 1, 3],
                vec![0, 1, 4],
                vec![0, 2, 3],
                vec![0, 2, 4],
                vec![0, 3, 4],
                vec![1, 2, 3],
                vec![1, 2, 4],
                vec![1, 3, 4],
                vec![2, 3, 4],
            ],
        );
    }

    #[test]
    fn choose_n_too_small() {
        assert_eq!(
            SimulationExplorer::default()
                .map(|mut sim| sim
                    .choose_n(3, FusedAssertingIterator::from(0..2))
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            vec![vec![0, 1]],
        );
    }

    #[test]
    fn choose_n_from_empty() {
        assert_eq!(
            SimulationExplorer::default()
                .map(|mut sim| sim
                    .choose_n(3, FusedAssertingIterator::from(0..0))
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            vec![vec![]],
        );
    }

    #[test]
    fn choose_n_exact() {
        assert_eq!(
            SimulationExplorer::default()
                .map(|mut sim| sim
                    .choose_n(3, FusedAssertingIterator::from(0..3))
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            vec![vec![0, 1, 2]],
        );
    }

    #[test]
    fn choose_0_from_some() {
        assert_eq!(
            SimulationExplorer::default()
                .map(|mut sim| sim
                    .choose_n(0, FusedAssertingIterator::from(0..5))
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            vec![vec![]],
        );
    }

    #[test]
    fn choose_0_from_none() {
        assert_eq!(
            SimulationExplorer::default()
                .map(|mut sim| sim
                    .choose_n(0, FusedAssertingIterator::from(0..0))
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            vec![vec![]],
        );
    }

    #[test]
    fn choose_unknown_size_empty() {
        let mut simex = SimulationExplorer::default();
        let mut simulation = simex.next_simulation().unwrap();
        assert!(simulation
            .choose_unknown_size(FusedAssertingIterator::from(0..0))
            .is_none());
        assert!(simex.next_simulation().is_none());
    }

    #[test]
    fn choose_unknown_size_from_single_element() {
        assert_eq!(
            SimulationExplorer::default()
                .map(|mut sim| sim
                    .choose_unknown_size(FusedAssertingIterator::from(0..1))
                    .unwrap())
                .collect::<Vec<_>>(),
            vec![0],
        );
    }

    #[test]
    fn choose_unknown_size() {
        assert_eq!(
            SimulationExplorer::default()
                .map(|mut sim| sim
                    .choose_unknown_size(FusedAssertingIterator::from(0..10))
                    .unwrap())
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        );
    }

    #[test]
    fn choose_n_unknown_size() {
        assert_eq!(
            SimulationExplorer::default()
                .map(|mut sim| sim
                    .choose_n_unknown_size(3, FusedAssertingIterator::from(0..5))
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            vec![
                vec![0, 1, 2],
                vec![0, 1, 3],
                vec![0, 1, 4],
                vec![0, 2, 3],
                vec![0, 2, 4],
                vec![0, 3, 4],
                vec![1, 2, 3],
                vec![1, 2, 4],
                vec![1, 3, 4],
                vec![2, 3, 4],
            ],
        );
    }

    #[test]
    fn choose_n_unknown_size_too_small() {
        assert_eq!(
            SimulationExplorer::default()
                .map(|mut sim| sim
                    .choose_n_unknown_size(3, FusedAssertingIterator::from(0..2))
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            vec![vec![0, 1]],
        );
    }

    #[test]
    fn choose_n_unknown_size_from_empty() {
        assert_eq!(
            SimulationExplorer::default()
                .map(|mut sim| sim
                    .choose_n_unknown_size(3, FusedAssertingIterator::from(0..0))
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            vec![vec![]],
        );
    }

    #[test]
    fn choose_n_unknown_size_exact() {
        assert_eq!(
            SimulationExplorer::default()
                .map(|mut sim| sim
                    .choose_n_unknown_size(3, FusedAssertingIterator::from(0..3))
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            vec![vec![0, 1, 2]],
        );
    }

    #[test]
    fn choose_0_unknown_size_from_some() {
        assert_eq!(
            SimulationExplorer::default()
                .map(|mut sim| sim
                    .choose_n_unknown_size(0, FusedAssertingIterator::from(0..5))
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            vec![vec![]],
        );
    }

    #[test]
    fn choose_0_unknown_size_from_none() {
        assert_eq!(
            SimulationExplorer::default()
                .map(|mut sim| sim
                    .choose_n_unknown_size(0, FusedAssertingIterator::from(0..0))
                    .collect::<Vec<_>>())
                .collect::<Vec<_>>(),
            vec![vec![]],
        );
    }

    #[test]
    fn choose_integer_0() {
        assert_eq!(SimulationExplorer::default()
                .map(|mut sim| sim.choose_integer(0, 0)).collect::<Vec<_>>(),
            vec![0],
        );
    }

    #[test]
    fn choose_integer_max() {
        assert_eq!(SimulationExplorer::default()
                .map(|mut sim| sim.choose_integer(usize::MAX, usize::MAX)).collect::<Vec<_>>(),
            vec![usize::MAX],
        );
    }

    #[test]
    fn choose_integer_0_to_1() {
        assert_eq!(SimulationExplorer::default()
                .map(|mut sim| sim.choose_integer(0, 1)).collect::<Vec<_>>(),
            vec![0, 1],
        );
    }

    #[test]
    fn choose_integer_1_to_2() {
        assert_eq!(SimulationExplorer::default()
                .map(|mut sim| sim.choose_integer(1, 2)).collect::<Vec<_>>(),
            vec![1, 2],
        );
    }

    #[test]
    fn choose_integer_0_to_2() {
        assert_eq!(SimulationExplorer::default()
                .map(|mut sim| sim.choose_integer(0, 2)).collect::<Vec<_>>(),
            vec![0, 1, 2],
        );
    }

    #[test]
    fn choose_integer_1_to_3() {
        assert_eq!(SimulationExplorer::default()
                .map(|mut sim| sim.choose_integer(1, 3)).collect::<Vec<_>>(),
            vec![1, 2, 3],
        );
    }

    #[test]
    fn choose_integer_1_to_13() {
        assert_eq!(SimulationExplorer::default()
                .map(|mut sim| sim.choose_integer(1, 13)).collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13],
        );
    }

    #[test]
    fn choose_integer_neg_1_to_0() {
        assert_eq!(SimulationExplorer::default()
                .map(|mut sim| sim.choose_integer(-1, 0)).collect::<Vec<_>>(),
            vec![-1, 0],
        );
    }

    #[test]
    fn choose_integer_neg_2_to_neg_1() {
        assert_eq!(SimulationExplorer::default()
                .map(|mut sim| sim.choose_integer(-2, -1)).collect::<Vec<_>>(),
            vec![-2, -1],
        );
    }

    #[test]
    fn choose_integer_neg_2_to_0() {
        assert_eq!(SimulationExplorer::default()
                .map(|mut sim| sim.choose_integer(-2, 0)).collect::<Vec<_>>(),
            vec![-2, -1, 0],
        );
    }

    #[test]
    fn choose_integer_neg_3_to_neg_1() {
        assert_eq!(SimulationExplorer::default()
                .map(|mut sim| sim.choose_integer(-3, -1)).collect::<Vec<_>>(),
            vec![-3, -2, -1],
        );
    }

    #[test]
    fn choose_integer_neg_13_to_neg_1() {
        assert_eq!(SimulationExplorer::default()
                .map(|mut sim| sim.choose_integer(-13, -1)).collect::<Vec<_>>(),
            vec![-13, -12, -11, -10, -9, -8, -7, -6, -5, -4, -3, -2, -1],
        );
    }

    #[test]
    fn choose_integer_min_to_max() {
        assert!(SimulationExplorer::default()
                .map(|mut sim| sim.choose_integer(i16::MIN, i16::MAX)).eq(i16::MIN..=i16::MAX));
    }

    #[test]
    #[should_panic(expected = "min <= max")]
    fn choose_integer_1_to_0() {
        assert_eq!(SimulationExplorer::default()
                .map(|mut sim| sim.choose_integer(1, 0)).collect::<Vec<_>>(),
            vec![1, 0],
        );
    }
}
