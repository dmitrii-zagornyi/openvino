import argparse
import concurrent.futures
import contextlib
import functools
import hashlib
import os
import queue
import re
import requests
import ssl
import sys
import threading
import time
import traceback
import types

from pathlib import Path


DOWNLOAD_TIMEOUT = 5 * 60
CHUNK_SIZE = 1 << 15 if sys.stdout.isatty() else 1 << 20


class JobContext:
    def __init__(self):
        self._interrupted = False

    def print(self, value, *, end='\n', file=sys.stdout, flush=False):
        raise NotImplementedError

    def printf(self, format, *args, file=sys.stdout, flush=False):
        self.print(format.format(*args), file=file, flush=flush)

    def subprocess(self, args, **kwargs):
        raise NotImplementedError

    def check_interrupted(self):
        if self._interrupted:
            raise RuntimeError("job interrupted")

    def interrupt(self):
        self._interrupted = True


class DirectOutputContext(JobContext):
    def print(self, value, *, end='\n', file=sys.stdout, flush=False):
        print(value, end=end, file=file, flush=flush)

    def subprocess(self, args, **kwargs):
        return subprocess.run(args, **kwargs).returncode == 0


class QueuedOutputContext(JobContext):
    def __init__(self, output_queue):
        super().__init__()
        self._output_queue = output_queue

    def print(self, value, *, end='\n', file=sys.stdout, flush=False):
        self._output_queue.put((file, value + end))

    def subprocess(self, args, **kwargs):
        with subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                              universal_newlines=True, **kwargs) as p:
            for line in p.stdout:
                self._output_queue.put((sys.stdout, line))
            return p.wait() == 0


class JobWithQueuedOutput():
    def __init__(self, context, output_queue, future):
        self._context = context
        self._output_queue = output_queue
        self._future = future
        self._future.add_done_callback(lambda future: self._output_queue.put(None))

    def complete(self):
        for file, fragment in iter(self._output_queue.get, None):
            print(fragment, end='', file=file, flush=True)  # for simplicity, flush every fragment

        return self._future.result()

    def cancel(self):
        self._context.interrupt()
        self._future.cancel()


def run_in_parallel(num_jobs, f, work_items):
    with concurrent.futures.ThreadPoolExecutor(num_jobs) as executor:
        def start(work_item):
            output_queue = queue.Queue()
            context = QueuedOutputContext(output_queue)
            return JobWithQueuedOutput(
                context, output_queue, executor.submit(f, context, work_item))

        jobs = list(map(start, work_items))

        try:
            return [job.complete() for job in jobs]
        except Exception:
            for job in jobs:
                job.cancel()
            raise


EVENT_EMISSION_LOCK = threading.Lock()


class Reporter:
    GROUP_DECORATION = '#' * 16 + '||'
    SECTION_DECORATION = '=' * 10
    ERROR_DECORATION = '#' * 10

    def __init__(self, job_context, *,
                 enable_human_output=True, enable_json_output=False, event_context={}):
        self.job_context = job_context
        self.enable_human_output = enable_human_output
        self.enable_json_output = enable_json_output
        self.event_context = event_context

    def print_group_heading(self, text):
        if not self.enable_human_output:
            return
        self.job_context.printf('{} {} {}', self.GROUP_DECORATION, text, self.GROUP_DECORATION[::-1])
        self.job_context.print('')

    def print_section_heading(self, format, *args):
        if not self.enable_human_output:
            return
        self.job_context.printf('{} {}', self.SECTION_DECORATION, format.format(*args), flush=True)

    def print_progress(self, format, *args):
        if not self.enable_human_output:
            return
        self.job_context.print(format.format(*args), end='\r' if sys.stdout.isatty() else '\n', flush=True)

    def end_progress(self):
        if not self.enable_human_output:
            return
        if sys.stdout.isatty():
            self.job_context.print('')

    def print(self, format='', *args, flush=False):
        if not self.enable_human_output:
            return
        self.job_context.printf(format, *args, flush=flush)

    def log_warning(self, format, *args, exc_info=False):
        if exc_info:
            self.job_context.print(traceback.format_exc(), file=sys.stderr, end='')
        self.job_context.printf("{} Warning: {}", self.ERROR_DECORATION, format.format(*args), file=sys.stderr)

    def log_error(self, format, *args, exc_info=False):
        if exc_info:
            self.job_context.print(traceback.format_exc(), file=sys.stderr, end='')
        self.job_context.printf("{} Error: {}", self.ERROR_DECORATION, format.format(*args), file=sys.stderr)

    def log_details(self, format, *args):
        print(self.ERROR_DECORATION, '    ', format.format(*args), file=sys.stderr)

    def emit_event(self, type, **kwargs):
        if not self.enable_json_output:
            return

        # We don't print machine-readable output through the job context, because
        # we don't want it to be serialized. If we serialize it, then the consumer
        # will lose information about the order of events, and we don't want that to happen.
        # Instead, we emit events directly to stdout, but use a lock to ensure that
        # JSON texts don't get interleaved.
        with EVENT_EMISSION_LOCK:
            json.dump({'$type': type, **self.event_context, **kwargs}, sys.stdout, indent=None)
            print()

    def with_event_context(self, **kwargs):
        return Reporter(
            self.job_context,
            enable_human_output=self.enable_human_output,
            enable_json_output=self.enable_json_output,
            event_context={**self.event_context, **kwargs},
        )


class TaggedBase:
    @classmethod
    def deserialize(cls, value):
        try:
            return cls.types[value['$type']].deserialize(value)
        except KeyError:
            raise DeserializationError('Unknown "$type": "{}"'.format(value['$type']))


class FileSource(TaggedBase):
    types = {}

    @classmethod
    def deserialize(cls, source):
        if isinstance(source, str):
            source = {'$type': 'http', 'url': source}
        return super().deserialize(source)


class FileSourceHttp(FileSource):
    RE_CONTENT_RANGE_VALUE = re.compile(r'bytes (\d+)-\d+/(?:\d+|\*)')

    def __init__(self, url):
        self.url = url
        self.name = url.split('/')[-1]

    @classmethod
    def deserialize(cls, source):
        return cls(validate_string('"url"', source['url']))

    def start_download(self, session, chunk_size, offset):
        headers = {}
        if offset != 0:
            headers['Range'] = 'bytes={}-'.format(offset)

        response = session.get(self.url, stream=True, timeout=DOWNLOAD_TIMEOUT, headers=headers)
        response.raise_for_status()

        if response.status_code == requests.codes.partial_content:
            match = self.RE_CONTENT_RANGE_VALUE.fullmatch(response.headers.get('Content-Range', ''))
            if not match:
                # invalid range reply; return a negative offset to make
                # the download logic restart the download.
                return None, -1

            return response.iter_content(chunk_size=chunk_size), int(match.group(1))

        # either we didn't ask for a range, or the server doesn't support ranges

        if 'Content-Range' in response.headers:
            # non-partial responses aren't supposed to have range information
            return None, -1

        return response.iter_content(chunk_size=chunk_size), 0


FileSource.types['http'] = FileSourceHttp


def process_download(reporter, chunk_iterable, progress, fd):
    start_time = time.monotonic()

    try:
        for chunk in chunk_iterable:
            reporter.job_context.check_interrupted()

            if chunk:
                duration = time.monotonic() - start_time
                progress.size += len(chunk)
                progress.hasher.update(chunk)

                if duration != 0:
                    speed = int(progress.size / (1024 * duration))
                else:
                    speed = '?'

                reporter.print_progress('... {} KB, {} KB/s, {} seconds passed',
                                        progress.size // 1024,
                                        speed,
                                        int(duration))
                reporter.emit_event('model_file_download_progress', size=progress.size)

                fd.write(chunk)
    finally:
        reporter.end_progress()


def try_download(reporter, fd, num_attempts, start_download):
    progress = types.SimpleNamespace(size=0)

    for attempt in range(num_attempts):
        if attempt != 0:
            retry_delay = 10
            reporter.print("Will retry in {} seconds...", retry_delay, flush=True)
            time.sleep(retry_delay)

        try:
            reporter.job_context.check_interrupted()

            chunk_iterable, continue_offset = start_download(offset=progress.size)

            if continue_offset not in {0, progress.size}:
                # Somehow we neither restarted nor continued from where we left off.
                # Try to restart.
                chunk_iterable, continue_offset = start_download(offset=0)
                if continue_offset != 0:
                    reporter.log_error("Remote server refuses to send whole file, aborting")
                    return None

            if continue_offset == 0:
                fd.seek(0)
                fd.truncate()
                progress.size = 0
                progress.hasher = hashlib.sha256()

            process_download(reporter, chunk_iterable, progress, fd)

            return progress.hasher.digest()
        except (requests.exceptions.RequestException, ssl.SSLError):
            reporter.log_error("Download failed", exc_info=True)

    return None


def try_retrieve(reporter, destination, file, num_attempts, start_download):
    reporter.print_section_heading('Downloading {}', destination)

    success = False

    with destination.open('w+b') as fd:
        actual_hash = try_download(reporter, fd, num_attempts, start_download)

        success = True

    if not actual_hash:
        try:
            os.remove(destination)
        except OSError:
            reporter.log_error("Remoe failed: {}", destination)

    reporter.print()
    return success


def download(reporter, args, session_factory, file):
    session = session_factory()
    reporter.emit_event('file_download_begin', file=file.name)

    output = args.output_dir
    output.mkdir(parents=True, exist_ok=True)

    destination = output / file.name

    if not try_retrieve(reporter, destination, file, args.num_attempts,
                        functools.partial(file.start_download, session, CHUNK_SIZE)):
        reporter.emit_event('file_download_end', file=file.name, successful=False)
        return False

    reporter.emit_event('file_download_end', file=file.name, successful=True)

    return True


class DownloaderArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        sys.stderr.write('error: %s\n' % message)
        self.print_help()
        sys.exit(2)


def positive_int_arg(value_str):
    try:
        value = int(value_str)
        if value > 0:
            return value
    except ValueError:
        pass

    raise argparse.ArgumentTypeError('must be a positive integer (got {!r})'.format(value_str))


# There is no evidence that the requests.Session class is thread-safe,
# so for safety, we use one Session per thread. This class ensures that
# each thread gets its own Session.
class ThreadSessionFactory:
    def __init__(self, exit_stack):
        self._lock = threading.Lock()
        self._thread_local = threading.local()
        self._exit_stack = exit_stack

    def __call__(self):
        try:
            session = self._thread_local.session
        except AttributeError:
            with self._lock:  # ExitStack might not be thread-safe either
                session = self._exit_stack.enter_context(requests.Session())
            self._thread_local.session = session
        return session


if __name__ == '__main__':
    parser = DownloaderArgumentParser()
    parser.add_argument('-o', '--output_dir', type=Path, metavar='DIR',
                        default=Path.cwd(), help='path where to save models')
    parser.add_argument('--num_attempts', type=positive_int_arg, metavar='N', default=1,
                        help='attempt each download up to N times')
    parser.add_argument('--progress_format', choices=('text', 'json'), default='text',
                        help='which format to use for progress reporting')
    parser.add_argument('-j', '--jobs', type=positive_int_arg, metavar='N', default=1,
                        help='how many downloads to perform concurrently')
    parser.add_argument('--type', choices=('prerequisites', 'ci_dependencies', 'packages', 'all'), default='all',
                        help='which set of files need to download')

    args = parser.parse_args()

    files = []

    if args.type == 'prerequisites' or args.type == 'all':
        with open('prerequisites.txt') as file:
            for line in file.read().splitlines():
                if line:
                    files.append(FileSourceHttp(line))
    if args.type == 'ci_dependencies' or args.type == 'all':
        with open('ci_dependencies.txt') as file:
            for line in file.read().splitlines():
                if line:
                    files.append(FileSourceHttp(line))
    if args.type == 'packages' or args.type == 'all':
        with open('packages.txt') as file:
            for line in file.read().splitlines():
                if line:
                    files.append(FileSourceHttp(line))

    def make_reporter(context):
        return Reporter(context,
                        enable_human_output=args.progress_format == 'text',
                        enable_json_output=args.progress_format == 'json')

    reporter = make_reporter(DirectOutputContext())

    reporter.print_group_heading('Downloading')
    with contextlib.ExitStack() as exit_stack:
        session_factory = ThreadSessionFactory(exit_stack)
        if args.jobs == 1:
            results = [download(reporter, args, session_factory, file)
                       for file in files]
        else:
            results = run_in_parallel(args.jobs,
                                      lambda context, file: download(
                                          make_reporter(context),
                                          args,
                                          session_factory,
                                          file),
                                      files)
