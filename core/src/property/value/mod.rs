pub mod lww;
pub mod pn_counter;
pub mod yrs;
pub use lww::LWW;
pub use pn_counter::PNCounter;
pub use yrs::YrsString;

pub trait ProjectedValue {
    type Projected;
    fn projected(&self) -> Self::Projected;
}
