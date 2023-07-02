mod hybrid;
mod memory;
#[cfg(test)]
mod test;

use crate::error::Result;

use std::fmt::Display;

/// 表示边界
use std::ops::{Bound, RangeBounds};


/// A scan range.
pub struct Range {
    start: Bound<u64>,
    end: Bound<u64>,
}

impl Range {
    /// 从给定的Rust范围创建一个新的范围。我们无法直接在scan()函数中使用RangeBounds，
    /// 因为这会阻止我们将其存储到一个trait对象中。
    pub fn from(range: impl RangeBounds<u64>) -> Self {
        Self {
            start: match range.start_bound() {
                Bound::Included(v) => Bound::Included(*v),
                Bound::Excluded(v) => Bound::Excluded(*v),
                Bound::Unbounded => Bound::Unbounded,
            },
            end: match range.end_bound() {
                Bound::Included(v) => Bound::Included(*v),
                Bound::Excluded(v) => Bound::Excluded(*v),
                Bound::Unbounded => Bound::Unbounded,
            }


        }
    }
}



/// A log store. Entry indexes are 1-based, to match Raft semantics.
pub trait Store: Display + Sync + Send {
    /// Appends a log entry, returning its index.
    fn append(&mut self, entry: Vec<u8>) -> Result<u64>;

    /// Commits log entries up to and including the given index, making them immutable.
    fn commit(&mut self, index: u64) -> Result<()>;

    /// Returns the committed index, if any.
    fn committed(&self) -> u64;

    /// Fetches a log entry, if it exists.
    fn get(&self, index: u64) -> Result<Option<Vec<u8>>>;

    /// Returns the number of entries in the log.
    fn len(&self) -> u64;

    /// Scans the log between the given indexes.
    fn scan(&self, range: Range) -> Scan;

    /// Returns the size of the log, in bytes.
    fn size(&self) -> u64;

    /// Truncates the log be removing any entries above the given index, and returns the
    /// highest index. Errors if asked to truncate any committed entries.
    fn truncate(&mut self, index: u64) -> Result<u64>;

    /// Gets a metadata value.
    fn get_metadata(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Sets a metadata value.
    fn set_metadata(&mut self, key: &[u8], value: Vec<u8>) -> Result<()>;

    /// Returns true if the log has no entries.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}



/// Iterator over a log range.
pub type Scan<'a> = Box<dyn Iterator<Item = Result<Vec<u8>>> + 'a>;



// TODO 
// TEST