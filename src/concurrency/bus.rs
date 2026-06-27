//! Types partagés pour le bus de signaux (côté marché binaire).

/// Côté du marché binaire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum Side {
    Up,
    Down,
}

impl Side {
    pub fn as_str(&self) -> &'static str {
        match self {
            Side::Up => "up",
            Side::Down => "down",
        }
    }
}
