use uuid::Uuid;

pub struct UuidGenerator {}

impl UuidGenerator {
    pub fn new() -> Self {
        Self {}
    }

    #[cfg(feature = "system")]
    pub fn new_uuid(&self) -> Uuid {
        Uuid::new_v4()
    }

    #[cfg(not(feature = "system"))]
    pub fn new_uuid(&self) -> Uuid {
        Uuid::nil()
    }
}
