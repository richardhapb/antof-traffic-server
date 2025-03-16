// Custom errors for Events
pub enum EventError<'a> {
    ApiRequestError,
    DatabaseError(&'a str),
    SerializeError(&'a str),
    DeserializeError(&'a str),
    RequestDataError(&'a str),
}
