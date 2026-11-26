//! What a Location of this technology is configured with: its declaration,
//! which `Configured` reads, built with the crate's `declaration` feature
//! alone and the code behind the default (ADR-0064, amendments 2026-10-22
//! and 2026-11-26), for the library the surfaces load to carry (open
//! problem 48). `user` and the name `virtual_host` are the `amqp`
//! technology's `presenting`, taken by that crate's own `declaration`
//! feature.

use amqp::presenting;
use xcore::settings::{Applies, Kind, Presence, Setting, Settings};

/// Every setting a Location takes beyond its address, the broker,
/// `host:5672`: where a Location connects. The password is the secret the
/// Location names, never a setting.
pub const SETTINGS: &Settings = &Settings {
    technology: env!("CARGO_PKG_NAME"),
    settings: &[
        Setting {
            name: "queue",
            kind: Kind::Text,
            presence: Presence::Required,
            meaning: "The queue a Receive Location consumes and a Send Location publishes to \
                      when a target names no queue.",
            applies: Applies::Both,
        },
        Setting {
            name: presenting::VIRTUAL_HOST,
            kind: Kind::Text,
            presence: Presence::Optional,
            meaning: "The broker's virtual host the login opens; the default one, `/`, \
                      when left out.",
            applies: Applies::Both,
        },
        presenting::USER,
        transport::configured::timeout(
            Applies::Both,
            None,
            "How long a peer that stops mid-frame is waited on, and how long a \
             quiet queue ends a receive; unbounded when left out.",
        ),
    ],
};

/// The schemes a Location's address is entered with to choose this technology.
pub const SCHEMES: &[&str] = &["rabbitmq"];
