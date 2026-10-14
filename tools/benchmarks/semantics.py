"""Explicitly audited workload contracts; unknown instrument bytes stay separate."""
import hashlib

# Reviewed fixtures: identical seeded DAG shapes (linear, diamonds, antichains,
# disjoint), cloned in-memory retriever inside the timed async compare loop,
# current-thread Tokio, DEFAULT_BUDGET. Event/Clock construction adapts to source
# APIs outside the timed region. Drain includes compare+iterator consumption;
# prepare includes batch validation+comparison+loaded-layer inspection.
AUDITED = {
    "85176dda5f6ba13cf14d9950c3017a27cd69e7f30397bb0ebaec5837b9f48c44": "debug",
    "018424d4a63d4230593b72cd82c2032133a00511420a3a4890f92f86926c8682": "assert",
    "aad4ea94a13eba036af21fe31548bc5d58681a7d05aebe686b28e6cc3b8e4768": "debug",
    "86c5824bc9f7b92473f20f073b7dc7a1aa3f4fe50288d7358c4bef07ec807df4": "assert",
    "b3da8cc22ffc72b2104f4c598ac9a0e879e1d1b683ac77b9d4c19d0e184e1fc1": "debug",
    "bc0cbf6f8fdc3a42638b20de286830e397ff0b45a3e1a41744bec8f68c8fe5ee": "assert",
    "34ab129d76ab85441cbedb8d0066718f24955eb0f48b6c66fbb8cc412b563b5b": "debug",
    "77e87f8a9c44d9ccbabf8751b948a573a2f0dea81409b2c41cf239923c780c73": "debug",
    "3c7483239346eaeb8645415a11444d04136c555a4a9b032e7d1df7fc374fc0c3": "debug",
    "66b559c320366dce4a02d256bea6e93a85c1e1e592a4e371b1aae23005aaefb6": "assert",
    "5e306e7efb5137b00f7b3f5054ef29c9c60493b0e1aa68010078ebea7bb253ed": "assert",
    "5ea96b0a0de569c4098de17584f570bb3947b8fc4abf76a3f4910461385234ea": "assert",
}


def audited_workloads(benchmark, names):
    assertions = AUDITED.get(hashlib.sha256(benchmark).hexdigest())
    if assertions is None:
        return {}
    result = {}
    for name in names:
        if name.startswith("compare/"):
            result[name] = f"dag-cloned-compare-{assertions}-v1"
        elif name.startswith("layers/diamond_chain_drain/"):
            result[name] = f"dag-cloned-diamond-drain-{assertions}-v1"
        elif name.startswith("layers/diamond_chain_prepare/"):
            result[name] = f"dag-cloned-diamond-prepare-{assertions}-v1"
    return result
