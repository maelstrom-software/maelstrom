//! Provide a mechanism for exhaustively exploring all possible simulations in a simulation space.
//!
//! The user is given a [`Simulation`] object, from which they can retrieve values (choices). On
//! later simulations, different values will be retrieved. The [`SimulationExplorer`] will keep
//! yielding simulations until all possible values have been returned.
#![deny(missing_docs)]

use num::integer::{Average, Integer};
use std::{
    collections::VecDeque,
    iter::{Fuse, FusedIterator},
};

/*  ____  _                 _       _   _             ____  _        _
 * / ___|(_)_ __ ___  _   _| | __ _| |_(_) ___  _ __ / ___|| |_ __ _| |_ ___
 * \___ \| | '_ ` _ \| | | | |/ _` | __| |/ _ \| '_ \\___ \| __/ _` | __/ _ \
 *  ___) | | | | | | | |_| | | (_| | |_| | (_) | | | |___) | || (_| | ||  __/
 * |____/|_|_| |_| |_|\__,_|_|\__,_|\__|_|\___/|_| |_|____/ \__\__,_|\__\___|
 *  FIGLET: SimulationState
 */

/// The underlying state of a simulation. It just consists of the script and our cursor within the
/// script.
///
/// The state is shared by `SimulationExplorer` and `Simulation`, implying that only a single
/// `Simulation` may be currently ongoing.
#[derive(Default)]
#[doc(hidden)]
struct SimulationState {
    script: Vec<bool>,
    cursor: usize,
}

impl SimulationState {
    /// Choose a bool.
    ///
    /// There are two cases. If we are at the beginning of the script, then we are just replaying
    /// what happened in the previous script. In that case, we just returned the scripted value and
    /// increment out cursor.
    ///
    /// Otherwise, we're in unexplored territory, and we always return false. Subsequent
    /// simulations will return true.
    ///
    /// In the first case, we have something like this:
    /// >    Before:
    /// >        01101
    /// >          ^
    /// >          |
    /// >        cursor
    /// >
    /// >    After:
    /// >        01101
    /// >          ^
    /// >          |
    /// >        cursor
    ///
    /// In the second case, we have something like this:
    /// >    Before:
    /// >        0101
    /// >           ^
    /// >           |
    /// >         cursor
    /// >
    /// >    After:
    /// >        01010
    /// >            ^
    /// >            |
    /// >          cursor
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

    /// End the current simulation and prepare for the next simulation, if there was one.
    ///
    /// If this function is called while the cursor is still within the script from the previous
    /// simulation, then we know we've hit some sort of non-determinism, and we panic. In other
    /// words, the previous simulation walked the exact same script up to (and potentially past)
    /// this point before it finished. So, if next_simulation is being called before we reach that
    /// same point, we know that the simulation must not be executing the same code.
    ///
    /// To update the script for the next simulation, we first try to reverse the last decision.
    /// However, if we've already done that, we reverse the decision before that, etc. The net
    /// result is to add one to the binary representation of the script, and then remove any
    /// trailing zeros. We remove the whole script, then we're done.
    ///
    /// Example one:
    /// >    Before:
    /// >        010
    /// >          ^
    /// >          |
    /// >        cursor
    /// >
    /// >    After:
    /// >        011
    /// >        ^
    /// >        |
    /// >      cursor
    ///
    /// Example two:
    /// >    Before:
    /// >        0101
    /// >           ^
    /// >           |
    /// >         cursor
    /// >
    /// >    After:
    /// >        011
    /// >        ^
    /// >        |
    /// >      cursor
    ///
    /// Example three:
    /// >    Before:
    /// >        0111
    /// >           ^
    /// >           |
    /// >         cursor
    /// >
    /// >    After:
    /// >        1
    /// >        ^
    /// >        |
    /// >      cursor
    ///
    /// Example four:
    /// >    Before:
    /// >        1111
    /// >           ^
    /// >           |
    /// >         cursor
    /// >
    /// >    After:
    /// >        Done
    fn end_current_simulation_and_prepare_next(&mut self) -> bool {
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

/*  ____  _                 _       _   _             _____            _
 * / ___|(_)_ __ ___  _   _| | __ _| |_(_) ___  _ __ | ____|_  ___ __ | | ___  _ __ ___ _ __
 * \___ \| | '_ ` _ \| | | | |/ _` | __| |/ _ \| '_ \|  _| \ \/ / '_ \| |/ _ \| '__/ _ \ '__|
 *  ___) | | | | | | | |_| | | (_| | |_| | (_) | | | | |___ >  <| |_) | | (_) | | |  __/ |
 * |____/|_|_| |_| |_|\__,_|_|\__,_|\__|_|\___/|_| |_|_____/_/\_\ .__/|_|\___/|_|  \___|_|
 *                                                              |_|
 *  FIGLET: SimulationExplorer
 */

/// An object that yields successive [`Simulation`]s until there are no more.
#[derive(Default)]
pub struct SimulationExplorer {
    state: Option<SimulationState>,
}

impl SimulationExplorer {
    /// Provide the next [`Simulation`], if there is one.
    pub fn next_simulation(&mut self) -> Option<Simulation<'_>> {
        match self.state {
            None => {
                self.state = Some(SimulationState::default());
                Some(Simulation {
                    state: self.state.as_mut().unwrap(),
                })
            }
            Some(ref mut state) => {
                if state.end_current_simulation_and_prepare_next() {
                    Some(Simulation { state })
                } else {
                    None
                }
            }
        }
    }

    /// Provide successive [`Simulation`]s to the given closure, and provide an iterator over the
    /// results.
    pub fn map<F, T>(&mut self, f: F) -> Map<'_, F>
    where
        F: for<'b> FnMut(Simulation<'b>) -> T,
    {
        Map { simex: self, f }
    }
}

/*  ____  _                 _       _   _
 * / ___|(_)_ __ ___  _   _| | __ _| |_(_) ___  _ __
 * \___ \| | '_ ` _ \| | | | |/ _` | __| |/ _ \| '_ \
 *  ___) | | | | | | | |_| | | (_| | |_| | (_) | | | |
 * |____/|_|_| |_| |_|\__,_|_|\__,_|\__|_|\___/|_| |_|
 *  FIGLET: Simulation
 */

/// An object that yields successive choices and which can be used to drive a simulation.
///
/// This object points into data owned by the [`SimulationExplorer`], and as such, only one
/// `Simulation` may be ongoing at a time (from a single `SimulationExplorer`).
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
    /// `Simulation` by small amount that is O(log(max-min)).
    ///
    /// # Panics
    /// This function will panic if `max` is less than `min`.
    ///
    /// # Examples
    /// ```
    /// use meticulous_simex::SimulationExplorer;
    /// assert!(SimulationExplorer::default()
    ///     .map(|mut sim| sim.choose_integer(0, 10))
    ///     .eq(0..=10));
    /// assert!(SimulationExplorer::default()
    ///     .map(|mut sim| sim.choose_integer(-1, 1))
    ///     .eq(-1..=1));
    /// ```
    pub fn choose_integer<T: Average + Copy + Integer>(&mut self, min: T, max: T) -> T {
        assert!(min <= max);
        self.choose_integer_unchecked(min, max)
    }

    /// Like `choose_integer`, but no check is done to asser that `min <= max`. If `min > max`,
    /// then the program will likely crash by running out of memory, or suffer some other similar
    /// fate.
    #[doc(hidden)]
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
    /// Let N be the length of the underlying iterator. This function does O(log(N)) work and then
    /// calls `Iterator::nth`. Thus, the time complexity can be as low as O(log(N)), and as high as
    /// O(N) (assuming `Iterator::nth` is no worse than O(N)).
    ///
    /// This function will increase the size of `Simulation` by a small amount that is O(log(N)).
    ///
    /// # Examples
    /// ```
    /// use meticulous_simex::SimulationExplorer;
    /// assert!(SimulationExplorer::default()
    ///     .map(|mut sim| sim.choose(0..10).unwrap())
    ///     .eq(0..10));
    /// assert!(SimulationExplorer::default()
    ///     .map(|mut sim| sim.choose([-1, 0, 1]).unwrap())
    ///     .eq(-1..=1));
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

    /// Choose a subset of up to `n` values from the given `ExactSizeIterator`. If the iterator
    /// contains fewer than `n` values, then all of the iterator's values will be returned.
    /// Otherwise, a subset of `n` of them will be chosen and returned.
    ///
    /// Values are returned in lexicographical order. So, choosing 2 from [1, 2, 3] will return, in
    /// order: [1, 2], [1, 3], then [2, 3].
    ///
    /// This function returns an iterator that returns the values in a lazy manner.
    ///
    /// Let N be the length of the underlying iterator. Each call to the `next()` method on the
    /// returned iterator does O(log(N)) work and then calls `Iterator::nth`. So, if all `n` items
    /// are retrieved from the iterator, a total of between O(n\* log(N)) and O(N) work will be done.
    /// The former bound holds when `Iterator:nth` is constant time, and the latter bound holds
    /// when `Iterator:nth` is linear. The reason it's not O(n\*N) in the latter case is that we
    /// will only ever consume the underlying iterator once.
    ///
    /// This function will increase the size of `Simulation` by a small amount that is O(log(N)).
    ///
    /// The returned iterator itself is of O(1) size.
    ///
    /// # Examples
    /// ```
    /// let mut simex = meticulous_simex::SimulationExplorer::default();
    /// let mut combinations = vec![];
    /// while let Some(mut simulation) = simex.next_simulation() {
    ///     combinations.push(Vec::from_iter(simulation.choose_n(3, 1..6)));
    /// }
    /// assert_eq!(
    ///     combinations,
    ///     vec![
    ///         [1, 2, 3],
    ///         [1, 2, 4],
    ///         [1, 2, 5],
    ///         [1, 3, 4],
    ///         [1, 3, 5],
    ///         [1, 4, 5],
    ///         [2, 3, 4],
    ///         [2, 3, 5],
    ///         [2, 4, 5],
    ///         [3, 4, 5],
    ///     ]
    /// );
    /// ```
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

    /// Like `Self::choose`, except the iterator doesn't have to implement `ExactSizeIterator`.
    ///
    /// Choose one value out of the given `Iterator`. If the iterator is empty, return `None`.
    ///
    /// Values are returned in the iterator's order.
    ///
    /// Let N be the length of the underlying iterator. The time complexity of this function is
    /// O(N).
    ///
    /// This function will increase the size of `Simulation` by a small amount that is O(log(N)).
    ///
    /// # Examples
    /// ```
    /// use meticulous_simex::SimulationExplorer;
    /// assert!(SimulationExplorer::default()
    ///     .map(|mut sim| sim.choose_unknown_size(0..10).unwrap())
    ///     .eq(0..10));
    /// assert!(SimulationExplorer::default()
    ///     .map(|mut sim| sim.choose_unknown_size([-1, 0, 1]).unwrap())
    ///     .eq(-1..=1));
    /// ```
    pub fn choose_unknown_size<T: IntoIterator>(&mut self, i: T) -> Option<T::Item> {
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

    /// Like `Self::choose_n`, except the iterator doesn't have to implement `ExactSizeIterator`.
    ///
    /// Choose a subset of up to `n` values from the given `Iterator`. If the iterator contains
    /// fewer than `n` values, then all of the iterator's values will be returned. Otherwise, a
    /// subset of `n` of them will be chosen and returned.
    ///
    /// Values are returned in lexicographical order. So, choosing 2 from [1, 2, 3] will return, in
    /// order: [1, 2], [1, 3], then [2, 3].
    ///
    /// This function returns an iterator that returns the values in a lazy manner.
    ///
    /// Let N be the length of the underlying iterator. If all `n` items are retrieved from the
    /// returned iterator, a total of O(N) work will be done.
    ///
    /// This function will increase the size of `Simulation` by an amount that is O(N).
    ///
    /// The iterator itself is of size O(n).
    ///
    /// # Examples
    /// ```
    /// let mut simex = meticulous_simex::SimulationExplorer::default();
    /// let mut combinations = vec![];
    /// while let Some(mut simulation) = simex.next_simulation() {
    ///     combinations.push(Vec::from_iter(simulation.choose_n(3, 1..6)));
    /// }
    /// assert_eq!(
    ///     combinations,
    ///     vec![
    ///         [1, 2, 3],
    ///         [1, 2, 4],
    ///         [1, 2, 5],
    ///         [1, 3, 4],
    ///         [1, 3, 5],
    ///         [1, 4, 5],
    ///         [2, 3, 4],
    ///         [2, 3, 5],
    ///         [2, 4, 5],
    ///         [3, 4, 5],
    ///     ]
    /// );
    /// ```
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

/*  __  __
 * |  \/  | __ _ _ __
 * | |\/| |/ _` | '_ \
 * | |  | | (_| | |_) |
 * |_|  |_|\__,_| .__/
 *              |_|
 *  FIGLET: Map
 */

/// An iterator that returns the results of simulations.
///
/// This struct is created by the [`map`](SimulationExplorer::map) method on
/// [`SimulationExplorer`]. See its documentation for more.
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

impl<'a, F, T> FusedIterator for Map<'a, F> where F: for<'b> FnMut(Simulation<'b>) -> T {}

/*   ____ _                          _   _
 *  / ___| |__   ___   ___  ___  ___| \ | |
 * | |   | '_ \ / _ \ / _ \/ __|/ _ \  \| |
 * | |___| | | | (_) | (_) \__ \  __/ |\  |
 *  \____|_| |_|\___/ \___/|___/\___|_| \_|
 *  FIGLET: ChooseN
 */

/// An iterator that returns chosen values from an underlying [`ExactSizeIterator`].
///
/// This struct is created by the [`choose_n`](Simulation::choose_n) method on [`Simulation`]. See
/// its documentation for more.
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
                self.iter
                    .nth(self.simulation.choose_integer_unchecked(0, extra))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}

impl<'a, 'b, T> FusedIterator for ChooseN<'a, 'b, T> where T: ExactSizeIterator {}

impl<'a, 'b, T> ExactSizeIterator for ChooseN<'a, 'b, T>
where
    T: ExactSizeIterator,
{
    fn len(&self) -> usize {
        self.n.min(self.iter.len())
    }
}

/*   ____ _                          _   _ _   _       _
 *  / ___| |__   ___   ___  ___  ___| \ | | | | |_ __ | | ___ __   _____      ___ __
 * | |   | '_ \ / _ \ / _ \/ __|/ _ \  \| | | | | '_ \| |/ / '_ \ / _ \ \ /\ / / '_ \
 * | |___| | | | (_) | (_) \__ \  __/ |\  | |_| | | | |   <| | | | (_) \ V  V /| | | |
 *  \____|_| |_|\___/ \___/|___/\___|_| \_|\___/|_| |_|_|\_\_| |_|\___/ \_/\_/ |_| |_|
 *  ____  _
 * / ___|(_)_______
 * \___ \| |_  / _ \
 *  ___) | |/ /  __/
 * |____/|_/___\___|
 *  FIGLET: ChooseNUnknownSize
 */

/// An iterator that returns chosen values from an underlying [`Iterator`].
///
/// The assumption is that the iterator is not an [`ExactSizeIterator`], otherwise [`ChooseN`]
/// would have been used.
///
/// This struct is created by the [`choose_n_unknown_size`](Simulation::choose_n_unknown_size)
/// method on [`Simulation`]. See its documentation for more.
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
                        if !self.reservoir.is_empty() {
                            self.n -= 1;
                        }
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

    fn size_hint(&self) -> (usize, Option<usize>) {
        // At a minimum, we will always return the whole of the reservoir, plus whatever else is in
        // the iterator, all bounded by self.n
        let (underlying_min, underlying_max) = self.iter.size_hint();
        let min = underlying_min
            .saturating_add(self.reservoir.len())
            .min(self.n);
        let max = if let Some(underlying_max) = underlying_max {
            underlying_max
                .saturating_add(self.reservoir.len())
                .min(self.n)
        } else {
            self.n
        };
        (min, Some(max))
    }
}

impl<'a, 'b, T> FusedIterator for ChooseNUnknownSize<'a, 'b, T> where T: Iterator {}

/*  _            _
 * | |_ ___  ___| |_ ___
 * | __/ _ \/ __| __/ __|
 * | ||  __/\__ \ |_\__ \
 *  \__\___||___/\__|___/
 *  FIGLET: tests
 */

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem;

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

        fn size_hint(&self) -> (usize, Option<usize>) {
            self.inner.size_hint()
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
        assert_eq!(actual, Vec::from_iter(0..10));
    }

    #[test]
    fn map() {
        assert!(SimulationExplorer::default()
            .map(|mut sim| sim.choose(FusedAssertingIterator::from(0..10)).unwrap())
            .eq(FusedAssertingIterator::from(0..10)));
    }

    #[test]
    fn map_is_fused() {
        let mut simex = SimulationExplorer::default();
        let mut iter = simex.map(|_| ());
        assert_eq!(iter.next(), Some(()));
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn choose_bool() {
        assert_eq!(
            Vec::from_iter(SimulationExplorer::default().map(|mut sim| sim.choose_bool())),
            vec![false, true],
        );
    }

    #[test]
    fn choose_n() {
        assert_eq!(
            Vec::from_iter(SimulationExplorer::default().map(|mut sim| Vec::from_iter(
                sim.choose_n(3, FusedAssertingIterator::from(0..5))
            ))),
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
            Vec::from_iter(SimulationExplorer::default().map(|mut sim| Vec::from_iter(
                sim.choose_n(3, FusedAssertingIterator::from(0..2))
            ))),
            vec![vec![0, 1]],
        );
    }

    #[test]
    fn choose_n_from_empty() {
        assert_eq!(
            Vec::from_iter(SimulationExplorer::default().map(|mut sim| Vec::from_iter(
                sim.choose_n(3, FusedAssertingIterator::from(0..0))
            ))),
            vec![vec![]],
        );
    }

    #[test]
    fn choose_n_exact() {
        assert_eq!(
            Vec::from_iter(SimulationExplorer::default().map(|mut sim| Vec::from_iter(
                sim.choose_n(3, FusedAssertingIterator::from(0..3))
            ))),
            vec![vec![0, 1, 2]],
        );
    }

    #[test]
    fn choose_0_from_some() {
        assert_eq!(
            Vec::from_iter(SimulationExplorer::default().map(|mut sim| Vec::from_iter(
                sim.choose_n(0, FusedAssertingIterator::from(0..5))
            ))),
            vec![vec![]],
        );
    }

    #[test]
    fn choose_0_from_none() {
        assert_eq!(
            Vec::from_iter(SimulationExplorer::default().map(|mut sim| Vec::from_iter(
                sim.choose_n(0, FusedAssertingIterator::from(0..0))
            ))),
            vec![vec![]],
        );
    }

    #[test]
    fn choose_n_is_fused() {
        let mut simex = SimulationExplorer::default();
        let mut simulation = simex.next_simulation().unwrap();
        let mut iter = simulation.choose_n(1, FusedAssertingIterator::from(0..2));
        assert_eq!(iter.next(), Some(0));
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn choose_n_exact_size() {
        let mut simex = SimulationExplorer::default();
        let mut simulation = simex.next_simulation().unwrap();
        let mut iter = simulation.choose_n(1, FusedAssertingIterator::from(0..2));
        assert_eq!(iter.len(), 1);
        assert_eq!(iter.next(), Some(0));
        assert_eq!(iter.len(), 0);
        assert_eq!(iter.next(), None);
        assert_eq!(iter.len(), 0);
    }

    #[test]
    fn choose_n_exact_size_with_short_underlying_iter() {
        let mut simex = SimulationExplorer::default();
        let mut simulation = simex.next_simulation().unwrap();
        let mut iter = simulation.choose_n(100, FusedAssertingIterator::from(0..2));
        assert_eq!(iter.len(), 2);
        assert_eq!(iter.next(), Some(0));
        assert_eq!(iter.len(), 1);
        assert_eq!(iter.next(), Some(1));
        assert_eq!(iter.len(), 0);
        assert_eq!(iter.next(), None);
        assert_eq!(iter.len(), 0);
        assert_eq!(iter.next(), None);
        assert_eq!(iter.len(), 0);
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
            Vec::from_iter(SimulationExplorer::default().map(|mut sim| {
                sim.choose_unknown_size(FusedAssertingIterator::from(0..1))
                    .unwrap()
            })),
            vec![0],
        );
    }

    #[test]
    fn choose_unknown_size() {
        assert_eq!(
            Vec::from_iter(SimulationExplorer::default().map(|mut sim| {
                sim.choose_unknown_size(FusedAssertingIterator::from(0..10))
                    .unwrap()
            })),
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        );
    }

    #[test]
    fn choose_n_unknown_size() {
        assert_eq!(
            Vec::from_iter(SimulationExplorer::default().map(|mut sim| Vec::from_iter(
                sim.choose_n_unknown_size(3, FusedAssertingIterator::from(0..5))
            ))),
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
            Vec::from_iter(SimulationExplorer::default().map(|mut sim| Vec::from_iter(
                sim.choose_n_unknown_size(3, FusedAssertingIterator::from(0..2))
            ))),
            vec![vec![0, 1]],
        );
    }

    #[test]
    fn choose_n_unknown_size_from_empty() {
        assert_eq!(
            Vec::from_iter(SimulationExplorer::default().map(|mut sim| Vec::from_iter(
                sim.choose_n_unknown_size(3, FusedAssertingIterator::from(0..0))
            ))),
            vec![vec![]],
        );
    }

    #[test]
    fn choose_n_unknown_size_exact() {
        assert_eq!(
            Vec::from_iter(SimulationExplorer::default().map(|mut sim| Vec::from_iter(
                sim.choose_n_unknown_size(3, FusedAssertingIterator::from(0..3))
            ))),
            vec![vec![0, 1, 2]],
        );
    }

    #[test]
    fn choose_0_unknown_size_from_some() {
        assert_eq!(
            Vec::from_iter(SimulationExplorer::default().map(|mut sim| Vec::from_iter(
                sim.choose_n_unknown_size(0, FusedAssertingIterator::from(0..5))
            ))),
            vec![vec![]],
        );
    }

    #[test]
    fn choose_0_unknown_size_from_none() {
        assert_eq!(
            Vec::from_iter(SimulationExplorer::default().map(|mut sim| Vec::from_iter(
                sim.choose_n_unknown_size(0, FusedAssertingIterator::from(0..0))
            ))),
            vec![vec![]],
        );
    }

    struct TestIterator<T> {
        script: Vec<(usize, Option<usize>, Option<T>)>,
        min: usize,
        max: Option<usize>,
        next: Option<T>,
    }

    impl<T> TestIterator<T> {
        fn new(mut script: Vec<(usize, Option<usize>, Option<T>)>) -> Self {
            let (min, max, next) = script.remove(0);
            TestIterator {
                script,
                min,
                max,
                next,
            }
        }
    }

    impl<T> Iterator for TestIterator<T> {
        type Item = T;

        fn next(&mut self) -> Option<Self::Item> {
            if self.script.is_empty() {
                self.next.take()
            } else {
                let (min, max, mut next) = self.script.remove(0);
                mem::swap(&mut next, &mut self.next);
                self.min = min;
                self.max = max;
                next
            }
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            (self.min, self.max)
        }
    }

    #[test]
    fn choose_n_unknown_size_size_hint_1() {
        let mut simex = SimulationExplorer::default();
        let mut simulation = simex.next_simulation().unwrap();
        let mut iter = simulation.choose_n_unknown_size(
            1,
            FusedAssertingIterator::from(TestIterator::new(vec![
                (0, None, Some(0)),
                (0, None, Some(1)),
                (0, None, Some(2)),
            ])),
        );
        assert_eq!(iter.size_hint(), (0, Some(1)));
        assert_eq!(iter.next(), Some(0));
        assert_eq!(iter.size_hint(), (0, Some(0)));
    }

    #[test]
    fn choose_n_unknown_size_size_hint_2() {
        let mut simex = SimulationExplorer::default();
        let mut simulation = simex.next_simulation().unwrap();
        let mut iter = simulation.choose_n_unknown_size(
            1,
            FusedAssertingIterator::from(TestIterator::new(vec![
                (3, None, Some(0)),
                (2, None, Some(1)),
                (1, None, Some(2)),
            ])),
        );
        assert_eq!(iter.size_hint(), (1, Some(1)));
        assert_eq!(iter.next(), Some(0));
        assert_eq!(iter.size_hint(), (0, Some(0)));
    }

    #[test]
    fn choose_n_unknown_size_size_hint_3() {
        let mut simex = SimulationExplorer::default();
        let mut simulation = simex.next_simulation().unwrap();
        let mut iter = simulation.choose_n_unknown_size(
            4,
            FusedAssertingIterator::from(TestIterator::new(vec![
                (1, Some(3), Some(0)),
                (1, Some(2), Some(1)),
                (1, Some(1), Some(2)),
                (0, Some(0), None),
            ])),
        );
        assert_eq!(iter.size_hint(), (1, Some(3)));
        assert_eq!(iter.next(), Some(0));
        assert_eq!(iter.size_hint(), (2, Some(2)));
        assert_eq!(iter.next(), Some(1));
        assert_eq!(iter.size_hint(), (1, Some(1)));
        assert_eq!(iter.next(), Some(2));
        assert_eq!(iter.size_hint(), (0, Some(0)));
    }

    #[test]
    fn choose_n_unknown_size_size_hint_4() {
        let mut simex = SimulationExplorer::default();
        let mut simulation = simex.next_simulation().unwrap();
        let mut iter = simulation.choose_n_unknown_size(
            4,
            FusedAssertingIterator::from(TestIterator::new(vec![
                (0, Some(100), Some(0)),
                (0, Some(100), Some(1)),
                (0, Some(100), Some(2)),
                (0, Some(0), None),
            ])),
        );
        assert_eq!(iter.size_hint(), (0, Some(4)));
        assert_eq!(iter.next(), Some(0));
        assert_eq!(iter.size_hint(), (2, Some(2)));
        assert_eq!(iter.next(), Some(1));
        assert_eq!(iter.size_hint(), (1, Some(1)));
        assert_eq!(iter.next(), Some(2));
        assert_eq!(iter.size_hint(), (0, Some(0)));
        assert_eq!(iter.next(), None);
        assert_eq!(iter.size_hint(), (0, Some(0)));
    }

    #[test]
    fn choose_n_unknown_size_size_hint_5() {
        let mut simex = SimulationExplorer::default();
        let mut simulation = simex.next_simulation().unwrap();
        let mut iter = simulation.choose_n_unknown_size(
            2,
            FusedAssertingIterator::from(TestIterator::new(vec![
                (0, Some(100), Some(0)),
                (0, Some(100), Some(1)),
                (0, Some(100), Some(2)),
                (0, Some(100), Some(3)),
                (0, Some(0), None),
            ])),
        );
        assert_eq!(iter.size_hint(), (0, Some(2)));
        assert_eq!(iter.next(), Some(0));
        assert_eq!(iter.size_hint(), (1, Some(1)));
        assert_eq!(iter.next(), Some(1));
        assert_eq!(iter.size_hint(), (0, Some(0)));
        assert_eq!(iter.next(), None);
        assert_eq!(iter.size_hint(), (0, Some(0)));
        assert_eq!(iter.next(), None);
        assert_eq!(iter.size_hint(), (0, Some(0)));
    }

    #[test]
    fn choose_n_unknown_size_is_fused() {
        let mut simex = SimulationExplorer::default();
        let mut simulation = simex.next_simulation().unwrap();
        let mut iter = simulation.choose_n_unknown_size(1, FusedAssertingIterator::from(0..2));
        assert_eq!(iter.next(), Some(0));
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn choose_integer_0() {
        assert_eq!(
            Vec::from_iter(SimulationExplorer::default().map(|mut sim| sim.choose_integer(0, 0))),
            vec![0],
        );
    }

    #[test]
    fn choose_integer_max() {
        assert_eq!(
            Vec::from_iter(
                SimulationExplorer::default()
                    .map(|mut sim| sim.choose_integer(usize::MAX, usize::MAX))
            ),
            vec![usize::MAX],
        );
    }

    #[test]
    fn choose_integer_0_to_1() {
        assert_eq!(
            Vec::from_iter(SimulationExplorer::default().map(|mut sim| sim.choose_integer(0, 1))),
            vec![0, 1],
        );
    }

    #[test]
    fn choose_integer_1_to_2() {
        assert_eq!(
            Vec::from_iter(SimulationExplorer::default().map(|mut sim| sim.choose_integer(1, 2))),
            vec![1, 2],
        );
    }

    #[test]
    fn choose_integer_0_to_2() {
        assert_eq!(
            Vec::from_iter(SimulationExplorer::default().map(|mut sim| sim.choose_integer(0, 2))),
            vec![0, 1, 2],
        );
    }

    #[test]
    fn choose_integer_1_to_3() {
        assert_eq!(
            Vec::from_iter(SimulationExplorer::default().map(|mut sim| sim.choose_integer(1, 3))),
            vec![1, 2, 3],
        );
    }

    #[test]
    fn choose_integer_1_to_13() {
        assert_eq!(
            Vec::from_iter(SimulationExplorer::default().map(|mut sim| sim.choose_integer(1, 13))),
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13],
        );
    }

    #[test]
    fn choose_integer_neg_1_to_0() {
        assert_eq!(
            Vec::from_iter(SimulationExplorer::default().map(|mut sim| sim.choose_integer(-1, 0))),
            vec![-1, 0],
        );
    }

    #[test]
    fn choose_integer_neg_2_to_neg_1() {
        assert_eq!(
            Vec::from_iter(SimulationExplorer::default().map(|mut sim| sim.choose_integer(-2, -1))),
            vec![-2, -1],
        );
    }

    #[test]
    fn choose_integer_neg_2_to_0() {
        assert_eq!(
            Vec::from_iter(SimulationExplorer::default().map(|mut sim| sim.choose_integer(-2, 0))),
            vec![-2, -1, 0],
        );
    }

    #[test]
    fn choose_integer_neg_3_to_neg_1() {
        assert_eq!(
            Vec::from_iter(SimulationExplorer::default().map(|mut sim| sim.choose_integer(-3, -1))),
            vec![-3, -2, -1],
        );
    }

    #[test]
    fn choose_integer_neg_13_to_neg_1() {
        assert_eq!(
            Vec::from_iter(
                SimulationExplorer::default().map(|mut sim| sim.choose_integer(-13, -1))
            ),
            vec![-13, -12, -11, -10, -9, -8, -7, -6, -5, -4, -3, -2, -1],
        );
    }

    #[test]
    fn choose_integer_min_to_max() {
        assert!(SimulationExplorer::default()
            .map(|mut sim| sim.choose_integer(i16::MIN, i16::MAX))
            .eq(i16::MIN..=i16::MAX));
    }

    #[test]
    #[should_panic(expected = "min <= max")]
    fn choose_integer_1_to_0() {
        assert_eq!(
            Vec::from_iter(SimulationExplorer::default().map(|mut sim| sim.choose_integer(1, 0))),
            vec![1, 0],
        );
    }
}
