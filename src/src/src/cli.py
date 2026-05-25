#!/usr/bin/env python3
import argparse, asyncio, logging, sys, yaml
from mirrorsync import MirrorSync, SyncConfig, SyncMode, SyncDirection, ConflictStrategy
from adapters import create_adapter

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")

def load_config(path):
    with open(path) as f:
        data = yaml.safe_load(f)
    return SyncConfig(
        name=data["name"],
        source_dsn=data["source"]["dsn"],
        target_dsn=data["target"]["dsn"],
        mode=SyncMode(data.get("mode", "incremental")),
        direction=SyncDirection(data.get("direction", "source_to_target")),
        conflict_strategy=ConflictStrategy(data.get("conflict_strategy", "latest_wins")),
        batch_size=data.get("batch_size", 1000),
        tables=data.get("tables"),
        exclude_tables=data.get("exclude_tables", []),
        watermark_column=data.get("watermark_column", "updated_at"),
        dry_run=data.get("dry_run", False),
    )

async def cmd_run(config_path, dry_run=False):
    config = load_config(config_path)
    if dry_run:
        config.dry_run = True
    engine = MirrorSync(config, create_adapter(config.source_dsn), create_adapter(config.target_dsn))

    @engine.on("pre_table")
    def on_pre(table, **_): print(f"  ⟳  Syncing {table}...")

    @engine.on("post_table")
    def on_post(table, count, **_): print(f"  ✓  {table}: {count:,} records")

    @engine.on("on_error")
    def on_err(table, error, **_): print(f"  ✗  {table}: {error}", file=sys.stderr)

    print(f"\n🔄 MirrorSync — {config.name}")
    if config.dry_run:
        print("   ⚠️  DRY RUN\n")
    stats = await engine.run()
    print(f"""
  Records synced:  {stats.synced:,}
  Errors:          {stats.errors:,}
  Duration:        {stats.duration_seconds:.1f}s
  Throughput:      {stats.records_per_second:.0f} rec/s
  Success rate:    {stats.success_rate:.1f}%
""")
    return 0 if stats.errors == 0 else 1

async def cmd_validate(config_path):
    try:
        config = load_config(config_path)
        print(f"✓ Config valid: {config.name}")
        src = create_adapter(config.source_dsn)
        tgt = create_adapter(config.target_dsn)
        async with src:
            tables = await src.list_tables()
            print(f"✓ Source connected — {len(tables)} tables")
        async with tgt:
            tables = await tgt.list_tables()
            print(f"✓ Target connected — {len(tables)} tables")
        print("\n✅ Ready to sync.")
        return 0
    except Exception as e:
        print(f"✗ {e}", file=sys.stderr)
        return 1

def main():
    parser = argparse.ArgumentParser(prog="mirrorsync")
    parser.add_argument("-c", "--config", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("run")
    subparsers.add_parser("validate")
    args = parser.parse_args()
    logging.getLogger().setLevel(args.log_level)
    if args.command == "run":
        sys.exit(asyncio.run(cmd_run(args.config, args.dry_run)))
    elif args.command == "validate":
        sys.exit(asyncio.run(cmd_validate(args.config)))
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
