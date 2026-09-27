use strum_macros::{AsRefStr, EnumString};

#[derive(AsRefStr, Debug, Clone, Copy, PartialEq, Eq, EnumString)]
#[strum(serialize_all = "UPPERCASE")]
pub enum Direction {
    Asc,
    Desc,
}

impl Direction {
    pub fn reverse(self) -> Self {
        match self {
            Direction::Asc => Direction::Desc,
            Direction::Desc => Direction::Asc,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Order {
    pub field: String,
    pub direction: Direction,
}

/// Which side of a cursor to paginate towards.
///
/// NOTE: the `AsRefStr` serialization (`<=` / `>=`) is the legacy inclusive comparator that
/// assumes a descending listing; it is kept for existing callers. New code should use
/// [`CursorDirection::comparator`], which is strict and accounts for the listing direction.
#[derive(AsRefStr, Debug, Clone, Copy, PartialEq, Eq, EnumString)]
pub enum CursorDirection {
    #[strum(serialize = "<=")]
    After,
    #[strum(serialize = ">=")]
    Before,
}

impl CursorDirection {
    /// Strict comparison operator that selects rows positioned after (or before) the cursor in a
    /// listing ordered by `listing` direction.
    pub fn comparator(self, listing: Direction) -> &'static str {
        match (self, listing) {
            (CursorDirection::After, Direction::Desc)
            | (CursorDirection::Before, Direction::Asc) => "<",
            (CursorDirection::After, Direction::Asc)
            | (CursorDirection::Before, Direction::Desc) => ">",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_comparator_follows_listing_direction() {
        assert_eq!(CursorDirection::After.comparator(Direction::Desc), "<");
        assert_eq!(CursorDirection::After.comparator(Direction::Asc), ">");
        assert_eq!(CursorDirection::Before.comparator(Direction::Desc), ">");
        assert_eq!(CursorDirection::Before.comparator(Direction::Asc), "<");
        // legacy inclusive serialization is unchanged for existing callers
        assert_eq!(CursorDirection::After.as_ref(), "<=");
        assert_eq!(CursorDirection::Before.as_ref(), ">=");
    }
}
