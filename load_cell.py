# autopa calls:
#   lc.name, lc.sensor.get_samples_per_second()
#   lc.get_collector().start_collecting(min_time=t0)
#   lc.get_collector().collect_until(end_time) -> (rows, errors)
#   lc.get_collector().is_started            (settable; autopa releases with it)
# rows are 4-column [time_s, grams, counts, tare]; autopa uses force = -(c-tare).
#
# ============================ HOW SAMPLES ARE OBTAINED =====================
# Tested on firmware QD_Q2C_01.01.02.04.
#
#  * Sensor object = probe_air.sensor_helper (only probe_air is a registered
#    Klipper object). Has .oid, .query_cs1237_end_cmd, .read_origin_data.
#
#  * There appears to be no bulk stream on this firmware (or I couldn't make it 
#    work). QIDI's sensor_helper.bulk_queue listens for the message name 
#    "cs1237_data", which this firmware never sends, and the firmware does not 
#    emit the standard "sensor_bulk_data" either. Registering that handler 
#    yields 0 messages forever.
#
#  * The working data path is a BLOCKING QUERY:
#        resp = sensor.query_cs1237_end_cmd.send([oid, 0, 4])
#    which returns a dict whose 'data' is a 24-bit little-endian SIGNED value.
#
#  * Each query blocks the reactor, so sampling is driven from inside
#    collect_until()'s drain loop rather than from a callback.
#
#  * Timing: the query's value belongs to the moment the MCU sampled it, not the
#    moment the reply landed. We timestamp with print-time, offset back by
#    half of the measured round trip.
#
# Signs/units: the CS1237 reports a PUSH as a NEGATIVE count change (stock log
#   "WEIGHT:CS1237 ZERO:-3.28"; read_origin_data ~ -1.42e6). autopa does 
#   force = -(counts - tare) and therefore reads a push as POSITIVE, since a push
#   drives counts BELOW the rest tare.
#
# Tested: Qidi Q2, firmware QD_Q2C_01.01.02.02. See README.md.

import logging
import math

BPS = 4
# A jump larger than this between CONSECUTIVE polls is treated as a bad
# read and discarded. The largest I've seen on my machine is about 30k.
BAD_READ_CAP = 150_000
BATCH_INTERVAL = 0.1        # webhooks batch cadence (matches stock bulk_sensor)
UI_POLLS_PER_BATCH = 12     # polls per 0.1 s batch -> ~120 Hz feed, 10 Hz push

# Absolute wall-clock bound on any single capture. A sweep is ~95 s and a decay
# ~15 s. This exists so a mis-armed timer can never hang Klipper.
MAX_CAPTURE_WAIT_S = 180.0

# Measured on my Q2 with query_cs1237_end_cmd: ~3.9 ms round trip, so the
# link's theopretical ceiling is ~250 Hz. The default targets 120 Hz: comfortably 
# inside the ceiling, with room for Klipper's other reactor work between samples.
NOMINAL_HZ = 120.0
MAX_POLL_HZ = 240.0


def _to_float(i, default=None):
    try:
        f = float(i)
    except (TypeError, ValueError):
        return default
    if f != f or math.isinf(f):
        return default
    return f


def _as_bytes(i):
    if isinstance(i, (bytes, bytearray)): return bytes(i)
    if isinstance(i, str): return i.encode('latin-1', 'replace')
    return None


def decode_sample(sample):
    """
    24-bit little-endian signed value; byte 3 is a 0x00 pad.
    """
    if not sample or len(sample) < 3:
        return None
    b0, b1, b2 = sample[0], sample[1], sample[2]
    u = b0 | (b1 << 8) | (b2 << 16)
    return u - 0x1000000 if u & 0x800000 else u


class QidiCS1237Collector:
    """autopa-facing collector; one instance reused across runs."""

    def __init__(self, shim):
        self._shim = shim
        self._samples = []        # [t, grams, counts, tare]
        self._errors = 0
        self._min_time = 0.0
        self._tare_counts = 0.0
        self._tare_source = ''
        self.is_started = False

    # autopa collector protocol
    def start_collecting(self, min_time=0.0, deadline_print_time=None):
        self._samples = []
        self._errors = 0
        self._min_time = min_time
        self.is_started = True
        # One tare read, taken while the toolhead is at rest
        raw = self._shim._get_raw()
        if raw is not None:
            self._tare_counts = raw
            self._tare_source = 'one-shot poll'
            self._shim._set_tare(raw, source='one-shot poll')
        else:
            self._tare_source = 'unavailable'
        self._shim._note_run_start()
        self._shim._start_capture_timer(self, deadline_print_time)

    def collect_until(self, end_time):
        """Wait for the capture timer to finish sampling the queued motion.

        autopa issues its moves with run_script_from_command(), which BLOCKS the
        reactor until they complete. So by the time we are called the motion has
        already run; polling here would only capture the tail. Sampling must
        happen from a reactor timer installed at start_collecting() time, which
        runs WHILE the moves execute. All this method does is wait for that
        timer to drain and then hand back the samples.
        """
        reactor = self._shim.printer.get_reactor()
        try:
            # end_time is only known here (autopa queues its moves after calling
            # start_collecting), so arm the deadline now. 
            # If motion has already finished, the computed stop time is in the 
            # past and the timer stops on its first tick
            self._shim._arm_capture_deadline(end_time)
            hard_stop = reactor.monotonic() + MAX_CAPTURE_WAIT_S
            while self._shim._capture_active:
                if reactor.monotonic() > hard_stop:
                    logging.error("load_cell: capture exceeded %.0fs; "
                                  "stopping timer", MAX_CAPTURE_WAIT_S)
                    self._shim._capture_active = False
                    break
                reactor.pause(reactor.monotonic() + 0.02)
        finally:
            self._shim._stop_capture_timer()
            self.is_started = False
            for r in self._samples:
                r[3] = self._tare_counts
            self._shim._note_run_end(self._samples)
        rows = [r for r in self._samples if r[0] <= end_time]
        return rows, self._errors

    # one polled sample
    def _poll_once(self):
        shim = self._shim
        get = shim._poll_raw
        if get is None:
            self._errors += 1
            return
        try:
            counts, stamp = get()
        except Exception:
            self._errors += 1
            return
        if counts is None:
            self._errors += 1
            return
        if stamp is None or stamp < self._min_time:
            return
        # a huge jump is a bad read, not force
        if self._samples and abs(counts - self._samples[-1][2]) > BAD_READ_CAP:
            self._errors += 1
            return
        grams_div = shim.counts_per_gf or 0.0
        grams = (counts / grams_div) if grams_div else 0.0
        self._samples.append([stamp, grams, counts, self._tare_counts])


class QidiCS1237Sensor:
    """Adapter presenting the sensor as an autopa-visible 'sensor'."""

    def __init__(self, shim):
        self._shim = shim

    def get_samples_per_second(self):
        return self._shim.get_samples_per_second()

    def get_status(self, eventtime=None):
        return self._shim.get_status(eventtime)


class LoadCell:
    """The [load_cell] object autopa binds to."""

    def __init__(self, config, shim):
        self.printer = config.get_printer()
        self.name = config.get_name()
        self.sensor = QidiCS1237Sensor(shim)
        self._shim = shim
        self._collector = QidiCS1237Collector(shim)

    def get_collector(self):
        return self._collector

    def get_sensor(self):
        return self.sensor

    def get_status(self, eventtime=None):
        return self._shim.get_status(eventtime)


class QidiProbeAirShim:
    """
    Sensor access, polled-sample primitive, tare, and diagnostics.
    """

    def __init__(self, config):
        self.config = config
        self.printer = config.get_printer()
        self.reactor = self.printer.get_reactor()
        self.name = config.get_name()

        self.counts_per_gf = config.getfloat('counts_per_gf', 0.0, minval=0.)
        self.reported_sps = config.getfloat('samples_per_second', 0., minval=0.)
        self.poll_hz = config.getfloat('poll_hz', NOMINAL_HZ,
                                       minval=5.0, maxval=MAX_POLL_HZ)

        self._probe_air = None
        self._sensor = None          # probe_air.sensor_helper
        self._raw_cmd = None         # query_cs1237_end_cmd
        self._oid = None
        self._mcu = None
        self._poll_raw = None        # bound closure, set once ready

        self._tare_counts = 0.0
        self._tare_source = 'none'
        self._detected_sps = 0.0
        self._poll_count = 0
        self._last_response_time_ms = 0.0
        self._run_samples = 0
        self._rate_window = []
        self._ui_client = None
        self._ui_collector = None
        self._capture_timer = None
        self._capture_active = False
        self._capture_stop_mono = 0.0
        self._capture_collector = None

        gcode = self.printer.lookup_object('gcode')
        gcode.register_command('LOAD_CELL_DEBUG', self.cmd_LOAD_CELL_DEBUG,
                               desc="Qidi CS1237 shim diagnostics")
        gcode.register_command('LOAD_CELL_TARE', self.cmd_LOAD_CELL_TARE,
                               desc="Show the tare autopa will subtract")
        gcode.register_command('LOAD_CELL_PROBE_STREAM',
                               self.cmd_LOAD_CELL_PROBE_STREAM,
                               desc="Poll the CS1237 briefly and report the "
                                    "sample rate (no bulk stream exists here)")

        self.printer.register_event_handler('klippy:ready', self._handle_ready)

    # setup
    def _handle_ready(self):
        pa = self.printer.lookup_object('probe_air', None)
        if pa is None:
            raise self.printer.config_error(
                "[load_cell] requires QIDI's [probe_air] section in printer.cfg "
                "(the Q2's load cell). Not found.")
        self._probe_air = pa
        sensor = getattr(pa, 'sensor_helper', None)
        if sensor is None:
            raise self.printer.config_error(
                "[load_cell] probe_air has no sensor_helper; cannot reach the "
                "CS1237.")
        self._sensor = sensor
        self._oid = getattr(sensor, 'oid', None)
        self._raw_cmd = getattr(sensor, 'query_cs1237_end_cmd', None)
        gm = getattr(sensor, 'get_mcu', None)
        self._mcu = gm() if callable(gm) else getattr(sensor, 'mcu', None)
        if self._raw_cmd is None or self._oid is None or self._mcu is None:
            logging.warning("load_cell: sensor missing query_cs1237_end_cmd/"
                            "/oid/mcu - polled sampling unavailable")
        else:
            self._poll_raw = self._make_poll()
        self._register_dump_force()
        lc = self.printer.lookup_object('load_cell', None)
        if lc is not None:
            self.printer.send_event('load_cell:tare', lc)

    def _register_dump_force(self):
        # Publish the polled force stream over the websocket so autopa's web UI
        # live-force chart works.
        try:
            webhooks = self.printer.lookup_object('webhooks')
            webhooks.register_mux_endpoint(
                "load_cell/dump_force", "load_cell", self.name,
                self._add_api_client)
            logging.info("load_cell: registered load_cell/dump_force for %s",
                         self.name)
        except Exception:
            logging.exception("load_cell: failed to register dump_force "
                              "(web UI live chart will not stream)")

    def _add_api_client(self, web_request):
        web_request.send({'header': ('time_s', 'grams', 'counts', 'tare')})
        client = QidiForceWebhooksClient(web_request)
        self._ui_client = client
        self._ui_collector = QidiCS1237Collector(self)
        self._ui_collector.is_started = True
        self._note_run_start()
        reactor = self.reactor
        period = BATCH_INTERVAL
        waketime = reactor.monotonic() + BATCH_INTERVAL

        def _tick(eventtime):
            c = self._ui_collector
            if c is None or not c.is_started:
                self._ui_collector = None
                return reactor.NEVER
            for _ in range(UI_POLLS_PER_BATCH):
                c._poll_once()
            rows = c._samples[-256:]
            if rows:
                client.handle_batch({'data': [list(r) for r in rows],
                                     'errors': c._errors})
                del c._samples[:-256]
            if client.cconn.is_closed():
                self._ui_collector = None
                return reactor.NEVER
            return eventtime + period

        reactor.register_timer(_tick, waketime)

    # capture timer
    def _arm_capture_deadline(self, deadline_print_time):
        """Set the absolute print-time deadline the capture timer stops at.

        Called from collect_until(), because that is the first point at which
        autopa's end_time is known (it queues its moves after start_collecting).
        """
        reactor = self.reactor
        stop_mono = None
        if deadline_print_time is not None and self._mcu is not None:
            try:
                now_mono = reactor.monotonic()
                now_pt = self._mcu.estimated_print_time(now_mono)
                delta = deadline_print_time - now_pt
                stop_mono = now_mono + max(0.0, delta)
            except Exception:
                stop_mono = None
        if stop_mono is None:
            stop_mono = reactor.monotonic() + MAX_CAPTURE_WAIT_S
        self._capture_stop_mono = min(stop_mono,
                                      reactor.monotonic() + MAX_CAPTURE_WAIT_S)
        logging.info("load_cell: capture deadline %.3f (%.2fs from now)",
                     self._capture_stop_mono,
                     self._capture_stop_mono - reactor.monotonic())
        return self._capture_stop_mono

    def _start_capture_timer(self, collector, deadline_print_time=None):
        """Poll on a reactor timer so samples are taken WHILE motion runs.

        autopa drives its moves with run_script_from_command(), which blocks the
        reactor; sampling only from collect_until() would capture just the tail. 
        A reactor timer is the only thing that runs during those blocking calls.

        We stop on an absolute print-time deadline passed in by the
        caller, plus a generous wall-clock backstop. Print time advances in real
        time regardless of reactor state, so it is a reliable terminator.
        """
        self._stop_capture_timer()
        reactor = self.reactor
        period = 1.0 / (self.poll_hz or NOMINAL_HZ)
        self._capture_active = True

        stop_mono = reactor.monotonic() + MAX_CAPTURE_WAIT_S
        if deadline_print_time is not None:
            stop_mono = self._arm_capture_deadline(deadline_print_time)
        self._capture_stop_mono = stop_mono

        def _tick(eventtime):
            if collector is not self._capture_collector:
                self._capture_active = False
                return reactor.NEVER
            if eventtime >= self._capture_stop_mono:
                self._capture_active = False
                logging.info("load_cell: capture timer done, %d samples",
                             len(collector._samples))
                return reactor.NEVER
            collector._poll_once()
            return eventtime + period

        self._capture_collector = collector
        self._capture_timer = reactor.register_timer(_tick, reactor.NOW)
        return True

    def _stop_capture_timer(self):
        if self._capture_timer is not None:
            try:
                self.reactor.unregister_timer(self._capture_timer)
            except Exception:
                pass
            self._capture_timer = None
        self._capture_active = False
        self._capture_stop_mono = 0.0
        self._capture_collector = None

    def _make_poll(self):
        cmd = self._raw_cmd
        oid = self._oid
        mcu = self._mcu
        reactor = self.reactor

        def poll():
            t0 = reactor.monotonic()
            resp = cmd.send([oid, 0, 4])
            t1 = reactor.monotonic()
            self._last_response_time_ms = (t1 - t0) * 1000.0
            self._poll_count += 1
            # rolling achieved rate over the last ~1 s of polls
            if self._rate_window and (t1 - self._rate_window[0][0]) > 1.0:
                self._rate_window.append((t1, None))
                span = t1 - self._rate_window[0][0]
                if span > 0:
                    self._detected_sps = (len(self._rate_window) - 1) / span
                del self._rate_window[0]
            else:
                self._rate_window.append((t1, None))
                if len(self._rate_window) > 400:
                    del self._rate_window[0]
            host = t0 + (t1 - t0) / 2 # round trip latency / 2 + t0
            sample = _as_bytes(resp.get('data')) if isinstance(resp, dict) else None
            counts = decode_sample(sample)
            if counts is None:
                return None, None
            try:
                stamp = mcu.estimated_print_time(host)
            except Exception:
                stamp = host
            return counts, stamp

        return poll

    # tare
    def _set_tare(self, tare, source=''):
        self._tare_counts = tare
        self._tare_source = source

    def _get_raw(self):
        """One-shot read_origin_data(). Never called on a timer."""
        for holder in (self._sensor, self._probe_air):
            if holder is None:
                continue
            fn = getattr(holder, 'read_origin_data', None)
            if fn is None:
                continue
            try:
                return _to_float(fn())
            except Exception:
                continue
        return None

    def _note_run_start(self):
        self._poll_count = 0
        self._run_samples = 0
        self._first_stamp = None
        self._last_stamp = None

    def _note_run_end(self, rows):
        self._run_samples = len(rows)
        if len(rows) >= 2:
            span = rows[-1][0] - rows[0][0]
            if span > 0:
                self._detected_sps = (len(rows) - 1) / span

    # status
    def get_samples_per_second(self):
        if self.reported_sps:
            return self.reported_sps
        return self._detected_sps

    def get_status(self, eventtime=None):
        return {
            'counts_per_gf': self.counts_per_gf,
            'tare': self._tare_counts,
            'tare_source': self._tare_source,
            'samples_per_second': self.get_samples_per_second(),
            'polls': self._poll_count,
            'run_samples': self._run_samples,
            'last_rtt_ms': self._last_response_time_ms,
        }

    # commands
    cmd_LOAD_CELL_DEBUG_help = (
        "Qidi CS1237 shim state: sensor, polling rate, last round-trip, tare. "
        "There is no bulk stream on this firmware - samples come from blocking "
        "queries issued only during a calibration run.")
    def cmd_LOAD_CELL_DEBUG(self, gcmd):
        gcmd.respond_info("Qidi CS1237 shim:")
        gcmd.respond_info("  sensor             : %s" % (
            'found' if self._poll_raw is not None else 'MISSING/limited'))
        gcmd.respond_info("  oid                : %s" % self._oid)
        gcmd.respond_info("  data path          : polled query_cs1237_end_cmd")
        gcmd.respond_info("  sample rate (sps)  : %.1f  (ceiling ~%.0f Hz)" % (
            self.get_samples_per_second() or 0., NOMINAL_HZ))
        gcmd.respond_info("  last round trip    : %.2f ms" % self._last_response_time_ms)
        gcmd.respond_info("  counts_per_gf      : %s" % (
            self.counts_per_gf or "(unset)"))
        gcmd.respond_info("  tare (counts)      : %.1f  [source: %s]" % (
            self._tare_counts, self._tare_source))
        gcmd.respond_info("  last run samples   : %d" % self._run_samples)
        raw = self._get_raw()      # single on-demand read
        gcmd.respond_info("  raw now (1-shot)  : %s" % (
            ('%.1f counts' % raw) if raw is not None else '(unavailable)'))

    cmd_LOAD_CELL_TARE_help = (
        "Show the tare (raw counts) autopa subtracts, and where it came from.")
    def cmd_LOAD_CELL_TARE(self, gcmd):
        gcmd.respond_info("Qidi CS1237 tare: %.1f counts [source: %s]"
                          % (self._tare_counts, self._tare_source))

    cmd_LOAD_CELL_PROBE_STREAM_help = (
        "Poll the CS1237 for a moment and report the achieved sample rate and "
        "value range. This firmware has no bulk stream, so this is the real "
        "data path. Does not heat or extrude. Optional: SECONDS=2.0, HZ=87.")
    def cmd_LOAD_CELL_PROBE_STREAM(self, gcmd):
        if self._poll_raw is None:
            raise gcmd.error("CS1237 polled path unavailable (need "
                             "query_cs1237_end_cmd, oid, mcu on "
                             "probe_air.sensor_helper)")
        secs = gcmd.get_float('SECONDS', 2.0, minval=0.2, maxval=10.0)
        hz = gcmd.get_float('HZ', NOMINAL_HZ, minval=5.0, maxval=MAX_POLL_HZ)
        interval = 1.0 / hz

        col = QidiCS1237Collector(self)
        col.is_started = True
        raw = self._get_raw()
        if raw is not None:
            col._tare_counts = raw
            self._set_tare(raw, source='one-shot poll')
        self._note_run_start()

        reactor = self.reactor
        start = reactor.monotonic()
        period = 1.0 / hz
        rows = []
        while reactor.monotonic() - start < secs:
            next_at = reactor.monotonic() + period
            col._poll_once()
            now = reactor.monotonic()
            if next_at > now:
                reactor.pause(next_at)
        col.is_started = False
        elapsed = reactor.monotonic() - start
        rows = col._samples
        self._note_run_end(rows)

        gcmd.respond_info("polled path        : query_cs1237_end_cmd")
        gcmd.respond_info("elapsed            : %.2f s" % elapsed)
        gcmd.respond_info("samples            : %d (errors %d)" % (
            len(rows), col._errors))
        if rows:
            vals = [r[2] for r in rows]
            gcmd.respond_info("value range        : %d .. %d counts" % (
                min(vals), max(vals)))
            gcmd.respond_info("first 8 raw        : %s" % vals[:8])
            gcmd.respond_info("achieved rate      : %.1f Hz (asked %.0f)" % (
                (len(rows) / elapsed if elapsed > 0 else 0.0), hz))
            gcmd.respond_info("last round trip    : %.2f ms" % self._last_response_time_ms)
        else:
            gcmd.respond_info("NO SAMPLES. The polled query returned nothing - "
                              "check that probe_air.sensor_helper exposes "
                              "query_cs1237_end_cmd and a valid oid.")


class QidiForceWebhooksClient:
    """Websocket client for load_cell/dump_force (autopa's live force chart).

    {'data': [[time_s, grams, counts, tare], ...], 'errors': N}
    """

    def __init__(self, web_request):
        self.cconn = web_request.get_client_connection()
        self.template = web_request.get_dict('response_template', {})

    def handle_batch(self, msg):
        if self.cconn.is_closed():
            return False      # falsy => BatchBulkHelper unregisters the client
        tmp = dict(self.template)
        tmp['params'] = msg
        self.cconn.send(tmp)
        return True


_SHIM = None

def _get_shim(config):
    global _SHIM
    if _SHIM is None:
        _SHIM = QidiProbeAirShim(config)
    return _SHIM


def load_config(config):
    shim = _get_shim(config)
    return LoadCell(config, shim)


def load_config_prefix(config):
    return load_config(config)
