use super::*;

/// A value-carrying test credential. Equality is full-value
/// (operational identity per the [`ContextData`] contract), so a
/// token refresh — same subject, new token — compares unequal and an
/// identical update compares equal.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TestCd {
    subject: u8,
    token: u8,
}
impl ContextData for TestCd {}

/// A context-shaped source — a private set owning its session,
/// attached to a registry — joins the registry's union while it
/// lives, and its members leave when it drops.
#[test]
fn attached_set_liveness() {
    let registry: SessionSet<TestCd> = SessionSet::new();
    let source: SessionSet<TestCd> = TestCd { subject: 1, token: 0 }.into();
    registry.attach(&source);
    assert_eq!(registry.sessions().len(), 1, "attached members join the union");

    let second: SessionSet<TestCd> = TestCd { subject: 2, token: 0 }.into();
    registry.attach(&second);
    assert_eq!(registry.sessions().len(), 2);

    drop(second);
    assert_eq!(registry.sessions().len(), 1, "a dropped source's members leave the union");
    drop(source);
    assert!(registry.sessions().is_empty());
}

/// Updates are visible to every holder and fire change subscribers
/// with the new value.
#[test]
fn update_is_shared_and_reactive() {
    let session = Session::new(TestCd { subject: 1, token: 1 });
    let holder = session.clone();

    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = seen.clone();
    let _guard = session.subscribe(move |value: TestCd| {
        sink.lock().unwrap().push(value.token);
    });

    session.update(TestCd { subject: 2, token: 2 });
    assert_eq!(holder.snapshot().token, 2, "holders read the new value");
    assert_eq!(seen.lock().unwrap().as_slice(), &[2], "subscriber fired with the new value");
}

/// A token refresh — same subject, new token — is a real change: it
/// compares unequal and fires the subscriber. An identical update is
/// a complete no-op: no notification.
#[test]
fn refresh_notifies_and_identical_update_is_a_noop() {
    let session = Session::new(TestCd { subject: 1, token: 1 });
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = seen.clone();
    let _guard = session.subscribe(move |value: TestCd| {
        sink.lock().unwrap().push(value.token);
    });

    let refreshed = TestCd { subject: 1, token: 2 };
    assert_ne!(session.snapshot(), refreshed, "a refresh carries a new token, so it compares unequal");
    session.update(refreshed);
    assert_eq!(seen.lock().unwrap().as_slice(), &[2], "the refresh fires the subscriber");

    session.update(TestCd { subject: 1, token: 2 });
    assert_eq!(seen.lock().unwrap().as_slice(), &[2], "an identical update does not notify");
    assert_eq!(session.snapshot().token, 2, "the stored value is unchanged");
}

/// A new session belongs to no set until something owns it.
#[test]
fn new_sessions_belong_to_no_set() {
    let set: SessionSet<TestCd> = SessionSet::new();
    let _infra = Session::new(TestCd { subject: 1, token: 0 });
    assert!(set.sessions().is_empty());
}

/// The set is a signal over the union of current values: it fires on
/// own, on any member's update, on attach, and when an attached
/// source drops.
#[test]
fn set_fires_on_membership_and_member_updates() {
    let set: SessionSet<TestCd> = SessionSet::new();
    let fired = Arc::new(Mutex::new(Vec::new()));
    let sink = fired.clone();
    let _guard = set.subscribe(move |current: Vec<TestCd>| {
        sink.lock().unwrap().push(current.iter().map(|cd| cd.token).collect::<Vec<_>>());
    });

    let a = Session::new(TestCd { subject: 1, token: 10 });
    set.own(&a);
    assert_eq!(fired.lock().unwrap().last(), Some(&vec![10]), "owning fires with the new union");

    a.update(TestCd { subject: 1, token: 11 });
    assert_eq!(fired.lock().unwrap().last(), Some(&vec![11]), "a member's update fires with its new value");

    let child: SessionSet<TestCd> = TestCd { subject: 2, token: 20 }.into();
    set.attach(&child);
    assert_eq!(fired.lock().unwrap().last(), Some(&vec![11, 20]), "attaching fires with the joined union");

    let count = fired.lock().unwrap().len();
    drop(child);
    assert_eq!(fired.lock().unwrap().last(), Some(&vec![11]), "a dropped source fires the shrunken union");
    assert_eq!(fired.lock().unwrap().len(), count + 1, "exactly the drop notification fired");
}

/// Two threads attaching opposite directions between the same pair
/// cannot both install their edge: whichever runs second sees the
/// first one's edge and refuses, so no cycle is ever built and the
/// walks that read the graph still terminate.
#[test]
fn racing_reciprocal_attaches_form_no_cycle() {
    for round in 0..64u8 {
        let left: SessionSet<TestCd> = TestCd { subject: 1, token: round }.into();
        let right: SessionSet<TestCd> = TestCd { subject: 2, token: round }.into();

        let start = Arc::new(std::sync::Barrier::new(2));
        let handles = [(left.clone(), right.clone()), (right.clone(), left.clone())].map(|(near, far)| {
            let start = start.clone();
            std::thread::spawn(move || {
                start.wait();
                near.attach(&far);
            })
        });
        for handle in handles {
            handle.join().expect("attach panicked");
        }

        let left_edges = left.0.attached.lock().unwrap().len();
        let right_edges = right.0.attached.lock().unwrap().len();
        assert_eq!(left_edges + right_edges, 1, "exactly one of the two reciprocal attaches installs its edge");

        // The walks terminate, and report the union the surviving edge
        // describes: the attaching set sees both members, the attached
        // one only its own.
        let (parent, child) = if left_edges == 1 { (&left, &right) } else { (&right, &left) };
        assert_eq!(parent.sessions().len(), 2, "the installed edge unions the child's member in");
        assert_eq!(child.sessions().len(), 1, "the refused edge left the child alone");
        assert!(parent.reaches(child));
        assert!(!child.reaches(parent));
    }
}

/// A cycle closed through a third set is refused too: reachability is
/// transitive, not a check of the immediate pair.
#[test]
fn attach_closing_a_longer_cycle_is_refused() {
    let a: SessionSet<TestCd> = TestCd { subject: 1, token: 0 }.into();
    let b: SessionSet<TestCd> = TestCd { subject: 2, token: 0 }.into();
    let c: SessionSet<TestCd> = TestCd { subject: 3, token: 0 }.into();

    a.attach(&b);
    b.attach(&c);
    assert_eq!(a.sessions().len(), 3, "a unions b and, through b, c");

    c.attach(&a);
    assert!(c.0.attached.lock().unwrap().is_empty(), "closing a -> b -> c -> a is refused");
    assert_eq!(c.sessions().len(), 1);
    assert_eq!(a.sessions().len(), 3, "the refusal changed nothing");
}

/// Peek reads the union without tracking; the porcelain and the
/// inherent accessors agree.
#[test]
fn porcelain_and_inherent_accessors_agree() {
    let set: SessionSet<TestCd> = SessionSet::new();
    let session = Session::new(TestCd { subject: 1, token: 1 });
    set.own(&session);
    assert_eq!(session.peek(), session.snapshot());
    assert_eq!(set.peek(), set.current());
    assert_eq!(set.peek(), vec![TestCd { subject: 1, token: 1 }]);
}
