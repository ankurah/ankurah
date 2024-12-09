//! Ankql is a query language for Ankurah in the style of Open Cypher / . The intention of which is to not totally reinvent the wheel, but to provide a
//! fast-to-implement query language for Ankurah which has only the necessary features to be useful.
//! <div>
//! <img src="data:image/jpeg;base64, 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"/>
//! </div>
//! <hr/>
//!
//! If this pans out, we might someday consider a more complete implementation and ISO/IEC 39075:2024 compliance.
