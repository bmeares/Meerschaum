# 0012. Cargo plugins can exchange typed frames without compiling the host

Status: accepted (5.1.7).

An ordinary Cargo plugin that reads or writes a Pipe needs its column types even when a
batch has no rows or a column contains only nulls. A datetime and a string can have the
same JSON representation. Cargo plugins can opt into a lightweight SDK that exchanges
typed batches over the existing Arrow C Stream interface. Meerschaum keeps the kernel
and its Polars dependency on the host for these builds.

Existing Cargo manifests keep the original SDK defaults. Both SDK configurations produce
ABI v1 libraries the same loader can consume. This lets plugin authors adopt the smaller
build independently of the installations that run their plugins.
