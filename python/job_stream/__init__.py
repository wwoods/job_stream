
from _job_stream import Job, runProcessor

import multiprocessing

def run(yamlPath):
    """Runs the given YAML file."""
    cpuCount = multiprocessing.cpu_count()
    runProcessor(yamlPath)
