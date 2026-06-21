"""
beam_worker.py — Scanner worker deployed on Beam Cloud.

Each invocation pulls a CIDR batch from the brain and runs hunt_v4.
Up to 30 concurrent on Beam Developer free tier ($30/mo credits).

Deploy:
    python3 beam_worker.py --deploy

Trigger one worker:
    python3 beam_worker.py --run <brain_url>

Trigger N workers:
    python3 beam_worker.py --run <brain_url> --count 10
"""
import argparse, sys

from beam import Image, PythonVersion, function

# Include hunt_v4.py in the container image via add_local_path
_image = (
    Image(python_version=PythonVersion.Python311)
    .add_local_path("hunt_v4.py")   # copies to /app/hunt_v4.py in container
)

@function(
    name="hunt-worker",
    cpu=1.0,
    memory=768,
    timeout=21600,     # 6h hard cap
    image=_image,
)
def run_scanner(brain_url: str, budget: int = 300000, workers: int = 600):
    """
    Pull CIDRs from brain_url, scan with hunt_v4, post results back.
    Runs until budget exhausted or timeout.
    """
    import subprocess, sys, os, time

    env = {**os.environ, "PYTHONUNBUFFERED": "1"}
    cmd = [
        sys.executable, "/app/hunt_v4.py",
        "--brain",   brain_url,
        "--workers", str(workers),
        "--budget",  str(budget),
    ]
    print(f"[beam-worker] starting: {' '.join(cmd)}", flush=True)
    proc = subprocess.run(cmd, env=env, timeout=21000)
    print(f"[beam-worker] exit code: {proc.returncode}", flush=True)
    return proc.returncode


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--deploy", action="store_true", help="Deploy function to Beam")
    parser.add_argument("--run", metavar="BRAIN_URL", help="Trigger worker(s) remotely")
    parser.add_argument("--count", type=int, default=1, help="Number of parallel workers")
    parser.add_argument("--budget", type=int, default=300000, help="IPs per worker batch")
    parser.add_argument("--workers", type=int, default=600, help="Asyncio workers per process")
    args = parser.parse_args()

    if args.deploy:
        run_scanner.deploy()
        print("Deployed. Trigger with: python3 beam_worker.py --run <brain_url> --count N")

    elif args.run:
        brain_url = args.run.rstrip("/")
        print(f"Launching {args.count} workers → {brain_url}")
        futures = [
            run_scanner.spawn(brain_url=brain_url, budget=args.budget, workers=args.workers)
            for _ in range(args.count)
        ]
        print(f"Spawned {len(futures)} workers. Results stream to brain.")
        # Don't block — workers POST results back to brain independently

    else:
        parser.print_help()
