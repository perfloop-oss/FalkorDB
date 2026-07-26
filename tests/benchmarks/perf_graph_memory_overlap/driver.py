#!/usr/bin/env python3
"""End-to-end GRAPH.MEMORY workloads for dense and disjoint label mappings."""

import argparse
import json
from pathlib import Path
import socket
import subprocess
import tempfile
import sys
import time


NODE_COUNT = 125_000
LABEL_COUNT = 16
SAMPLES = 100
PAYLOAD_BYTES = 256
FIXTURE_SHAPES = ("overlap", "nonoverlap")


def _graph_id(shape):
    return f"perf_graph_memory_{shape}"


def _membership_count(shape):
    if shape == "overlap":
        return NODE_COUNT * LABEL_COUNT
    return NODE_COUNT


def _nodes_in_label(shape, index):
    if shape == "overlap":
        return NODE_COUNT
    return NODE_COUNT // LABEL_COUNT + (index < NODE_COUNT % LABEL_COUNT)


class RespError(RuntimeError):
    pass


def _as_bytes(value):
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    if isinstance(value, int):
        return str(value).encode("ascii")
    raise TypeError(f"unsupported RESP argument type: {type(value)!r}")


def _encode_command(args):
    parts = [f"*{len(args)}\r\n".encode("ascii")]
    for arg in args:
        encoded = _as_bytes(arg)
        parts.append(f"${len(encoded)}\r\n".encode("ascii"))
        parts.append(encoded)
        parts.append(b"\r\n")
    return b"".join(parts)


def _read_line(reader):
    line = reader.readline()
    if not line or not line.endswith(b"\r\n"):
        raise RespError("truncated RESP reply")
    return line[:-2]


def _read_reply(reader):
    prefix = reader.read(1)
    if not prefix:
        raise RespError("connection closed while reading RESP reply")

    if prefix == b"+":
        return _read_line(reader).decode("utf-8")
    if prefix == b"-":
        raise RespError(_read_line(reader).decode("utf-8"))
    if prefix == b":":
        return int(_read_line(reader))
    if prefix == b"_":
        _read_line(reader)
        return None
    if prefix == b"$":
        length = int(_read_line(reader))
        if length == -1:
            return None
        value = reader.read(length)
        if len(value) != length or reader.read(2) != b"\r\n":
            raise RespError("truncated bulk RESP reply")
        return value.decode("utf-8")
    if prefix in (b"*", b"~", b">"):
        length = int(_read_line(reader))
        if length == -1:
            return None
        return [_read_reply(reader) for _ in range(length)]
    if prefix == b"%":
        length = int(_read_line(reader))
        return {_read_reply(reader): _read_reply(reader) for _ in range(length)}

    raise RespError(f"unsupported RESP reply prefix: {prefix!r}")


class RespClient:
    def __init__(self, socket_path):
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.socket.settimeout(120)
        self.socket.connect(str(socket_path))
        self.reader = self.socket.makefile("rb")

    def command(self, *args):
        self.socket.sendall(_encode_command(args))
        return _read_reply(self.reader)

    def close(self):
        self.reader.close()
        self.socket.close()


def _as_map(value):
    if isinstance(value, dict):
        return value
    if not isinstance(value, list) or len(value) % 2:
        raise RespError(f"expected map-shaped response, got {value!r}")
    return {value[index]: value[index + 1] for index in range(0, len(value), 2)}


def _validate_memory_reply(reply, shape):
    report = _as_map(reply)
    required = {
        "total_graph_sz_mb",
        "amortized_node_attributes_by_label_sz_mb",
        "amortized_unlabeled_nodes_attributes_sz_mb",
    }
    missing = required.difference(report)
    if missing:
        raise RespError(f"GRAPH.MEMORY reply missing keys: {sorted(missing)}")

    by_label = _as_map(report["amortized_node_attributes_by_label_sz_mb"])
    expected_labels = {f"L{index}" for index in range(LABEL_COUNT)}
    if set(by_label) != expected_labels:
        raise RespError(
            f"GRAPH.MEMORY labels differ from fixture: {sorted(by_label)!r}"
        )
    if by_label["L0"] <= 0:
        raise RespError("GRAPH.MEMORY did not account for sampled node attributes")
    if any(value < 0 for value in by_label.values()):
        raise RespError("GRAPH.MEMORY returned a negative label estimate")
    if shape == "overlap" and any(
        by_label[f"L{index}"] != 0 for index in range(1, LABEL_COUNT)
    ):
        raise RespError("overlap fixture did not assign attributes to the first label")
    if shape == "nonoverlap" and any(value <= 0 for value in by_label.values()):
        raise RespError("non-overlap fixture did not account for every label")

    return sum(by_label.values())


def _server_args(redis_server, module, dataset, socket_path, log_path):
    return [
        str(redis_server),
        "--port",
        "0",
        "--unixsocket",
        str(socket_path),
        "--unixsocketperm",
        "700",
        "--dir",
        str(dataset.parent),
        "--dbfilename",
        dataset.name,
        "--save",
        "",
        "--appendonly",
        "no",
        "--protected-mode",
        "no",
        "--loglevel",
        "warning",
        "--logfile",
        str(log_path),
        "--loadmodule",
        str(module),
    ]


def _log_tail(log_path):
    if not log_path.exists():
        return ""
    return log_path.read_text(errors="replace")[-4000:]


def _start_server(redis_server, module, dataset):
    if not redis_server.is_file():
        raise FileNotFoundError(f"redis-server not found: {redis_server}")
    if not module.is_file():
        raise FileNotFoundError(f"FalkorDB module not found: {module}")

    dataset.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        prefix="fdb-perf-", suffix=".sock", delete=False
    ) as socket_file:
        socket_path = Path(socket_file.name)
    log_path = dataset.with_suffix(".log")
    for path in (socket_path, log_path):
        path.unlink(missing_ok=True)

    process = subprocess.Popen(
        _server_args(redis_server, module, dataset, socket_path, log_path),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(
                f"redis-server exited during startup ({process.returncode}):\n"
                f"{_log_tail(log_path)}"
            )
        try:
            client = RespClient(socket_path)
            if client.command("PING") != "PONG":
                raise RespError("unexpected PING reply")
            return process, client, socket_path, log_path
        except (FileNotFoundError, ConnectionRefusedError, RespError, OSError):
            time.sleep(0.05)

    process.terminate()
    process.wait(timeout=30)
    raise RuntimeError(f"redis-server did not become ready:\n{_log_tail(log_path)}")


def _stop_server(process, client, socket_path):
    try:
        try:
            client.command("SHUTDOWN", "NOSAVE")
        except (RespError, BrokenPipeError, OSError):
            pass
        client.close()
        process.wait(timeout=30)
    except subprocess.TimeoutExpired:
        process.terminate()
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=30)
    finally:
        socket_path.unlink(missing_ok=True)


def _graph_queries(shape):
    payload = "x" * PAYLOAD_BYTES
    if shape == "overlap":
        labels = ":" + ":".join(f"L{index}" for index in range(LABEL_COUNT))
        return [
            f"UNWIND range(0, {NODE_COUNT - 1}) AS n "
            f"CREATE ({labels} {{payload: '{payload}'}})"
        ]

    return [
        f"UNWIND range(0, {_nodes_in_label(shape, index) - 1}) AS n "
        f"CREATE (:L{index} {{payload: '{payload}'}})"
        for index in range(LABEL_COUNT)
    ]


def prepare(redis_server, module, dataset, shape):
    dataset.unlink(missing_ok=True)
    process, client, socket_path, _ = _start_server(redis_server, module, dataset)
    try:
        graph_id = _graph_id(shape)
        for query in _graph_queries(shape):
            client.command("GRAPH.QUERY", graph_id, query)
        _validate_memory_reply(
            client.command("GRAPH.MEMORY", "USAGE", graph_id, "SAMPLES", SAMPLES),
            shape,
        )
        if client.command("SAVE") != "OK":
            raise RespError("SAVE did not return OK")
    finally:
        _stop_server(process, client, socket_path)

    if not dataset.is_file() or dataset.stat().st_size == 0:
        raise RuntimeError(f"fixture was not written: {dataset}")


def inspect(redis_server, module, dataset, shape):
    if not dataset.is_file():
        raise FileNotFoundError(f"fixture does not exist: {dataset}")

    process, client, socket_path, _ = _start_server(redis_server, module, dataset)
    try:
        graph_id = _graph_id(shape)
        _validate_memory_reply(
            client.command("GRAPH.MEMORY", "USAGE", graph_id, "SAMPLES", SAMPLES), shape
        )
        label_counts = []
        for index in range(LABEL_COUNT):
            reply = client.command(
                "GRAPH.QUERY", graph_id, f"MATCH (node:L{index}) RETURN count(node)"
            )
            try:
                label_counts.append(reply[1][0][0])
            except (IndexError, TypeError):
                raise RespError(f"unexpected label-count reply: {reply!r}") from None
    finally:
        _stop_server(process, client, socket_path)

    expected_counts = [_nodes_in_label(shape, index) for index in range(LABEL_COUNT)]
    if label_counts != expected_counts:
        raise RespError(f"fixture label counts differ: {label_counts!r}")
    print(f"fixture_node_label_memberships={sum(label_counts)}")
    print(f"fixture_sample_limit={SAMPLES}")


def sample(redis_server, module, dataset, shape):
    if not dataset.is_file():
        raise FileNotFoundError(f"fixture does not exist: {dataset}")

    process, client, socket_path, _ = _start_server(redis_server, module, dataset)
    try:
        graph_id = _graph_id(shape)
        _validate_memory_reply(
            client.command("GRAPH.MEMORY", "USAGE", graph_id, "SAMPLES", SAMPLES), shape
        )
        start = time.perf_counter_ns()
        reply = client.command("GRAPH.MEMORY", "USAGE", graph_id, "SAMPLES", SAMPLES)
        elapsed = time.perf_counter_ns() - start
        reported_node_attribute_mb = _validate_memory_reply(reply, shape)
    finally:
        _stop_server(process, client, socket_path)

    print(
        json.dumps(
            {"metric": f"graph_memory_{shape}_latency_ns", "value": elapsed}
        )
    )
    print(
        json.dumps(
            {
                "metric": "node_label_memberships",
                "value": _membership_count(shape),
            }
        )
    )
    print(
        json.dumps(
            {
                "metric": "reported_node_attribute_mb",
                "value": reported_node_attribute_mb,
            }
        )
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("prepare", "inspect", "sample"))
    parser.add_argument("--redis-server", required=True, type=Path)
    parser.add_argument("--module", required=True, type=Path)
    parser.add_argument("--dataset", required=True, type=Path)
    parser.add_argument("--shape", choices=FIXTURE_SHAPES, required=True)
    args = parser.parse_args()
    args.redis_server = args.redis_server.resolve()
    args.module = args.module.resolve()
    args.dataset = args.dataset.resolve()

    if args.mode == "prepare":
        prepare(args.redis_server, args.module, args.dataset, args.shape)
    elif args.mode == "inspect":
        inspect(args.redis_server, args.module, args.dataset, args.shape)
    else:
        sample(args.redis_server, args.module, args.dataset, args.shape)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"graph memory overlap benchmark failed: {error}", file=sys.stderr)
        sys.exit(1)
