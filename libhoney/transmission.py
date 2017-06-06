'''Transmission handles colleting and sending individual events to Honeycomb'''

from six.moves import queue
from six.moves.urllib.parse import urljoin
import json
import threading
import requests
import statsd
import datetime

sd = statsd.StatsClient(prefix="libhoney")
VERSION = "unset"  # set by libhoney


class Transmission():

    def __init__(self, max_concurrent_batches=10, block_on_send=False,
                 block_on_response=False, send_interval=0):
        self.max_concurrent_batches = max_concurrent_batches
        self.block_on_send = block_on_send
        self.block_on_response = block_on_response
        self.send_interval = send_interval

        session = requests.Session()
        session.headers.update({"User-Agent": "libhoney-py/"+VERSION})
        self.session = session

        # libhoney adds events to the pending queue for us to send
        self.pending = queue.Queue(maxsize=1000)
        # we hand back responses from the API on the responses queue
        self.responses = queue.Queue(maxsize=2000)

        self.threads = []
        for i in range(self.max_concurrent_batches):
            t = threading.Thread(target=self._sender)
            t.start()
            self.threads.append(t)

    def send(self, ev):
        '''send accepts an event and queues it to be sent'''
        sd.gauge("queue_length", self.pending.qsize())
        try:
            if self.block_on_send:
                self.pending.put(ev)
            else:
                self.pending.put_nowait(ev)
            sd.incr("messages_queued")
        except queue.Full:
            response = {
                "status_code": 0,
                "duration": 0,
                "metadata": ev.metadata,
                "body": "",
                "error": "event dropped; queue overflow",
            }
            if self.block_on_response:
                self.responses.put(response)
            else:
                try:
                    self.responses.put_nowait(response)
                except queue.Full:
                    # if the response queue is full when trying to add an event
                    # queue is full response, just skip it.
                    pass
            sd.incr("queue_overflow")

    def _sender(self):
        '''_sender is the control loop for each sending thread'''
        last_send = get_now()
        events = []
        while True:
            ev = self.pending.get()
            if ev is None:
                self._send_batch(events)
                break
            if self.send_interval > 0:
                current_time = get_now()
                # TODO - .seconds???
                if (current_time - last_send).seconds >= self.send_interval:
                    self._send_batch(events)
                    last_send = current_time
                    events = []  # TODO(an): is this a performance/memory problem?
                else:
                    events.append(ev)  # TODO(an): error checking for size
                                       # TODO(an): maybe sort by dataset here?
            else:
                self._send(ev)


    # TODO(an): events vs ev is inconsistent variable name
    # also a lot of the _send code is duplicated here because i didn't want to
    # spend too much time refactoring during this interview
    def _send_batch(self, events):
        '''_send_batch should only be called from sender and sends all events
           at once through the batch API'''
        if not events:
            return

        # TODO(an): for interview, going to assume all of the events have the
        # same dataset, but we should check the dataset for every individual event
        api_host = events[0].api_host
        writekey = events[0].writekey
        print writekey
        dataset = events[0].dataset
        url = urljoin(urljoin(api_host, "1/batch/"), dataset)

        # TODO(an): does json.dumps need to be extracted away like in
        # fieldholders - i don't think so?
        data = json.dumps([{'data': ev._fields._data} for ev in events])
        print data

        req = requests.Request('POST', url, data=data) # is the data= necessary 
        req.headers.update({"X-Honeycomb-Team": writekey})
        preq = self.session.prepare_request(req)
        responses = self.session.send(preq)

        # TODO(an): check for errors in all of the responses - it's an array
        #
        # TODO(an): send all responses to queue


    def _send(self, ev):
        '''_send should only be called from sender and sends an individual
            event to Honeycomb'''
        start = get_now()
        url = urljoin(urljoin(ev.api_host, "/1/events/"), ev.dataset)
        req = requests.Request('POST', url, data=str(ev))
        event_time = ev.created_at.isoformat()
        if ev.created_at.tzinfo is None:
            event_time += "Z"
        req.headers.update({
            "X-Event-Time": event_time,
            "X-Honeycomb-Team": ev.writekey,
            "X-Honeycomb-SampleRate": str(ev.sample_rate)})
        preq = self.session.prepare_request(req)
        resp = self.session.send(preq)
        if (resp.status_code == 200):
            sd.incr("messages_sent")
        else:
            sd.incr("send_errors")
        dur = get_now() - start
        response = {
            "status_code": resp.status_code,
            "duration": dur.total_seconds() * 1000,  # report in milliseconds
            "metadata": ev.metadata,
            "body": resp.text,
            "error": "",
        }
        if self.block_on_response:
            self.responses.put(response)
        else:
            try:
                self.responses.put_nowait(response)
            except queue.Full:
                pass

    def close(self):
        '''call close to send all in-flight requests and shut down the
            senders nicely. Times out after max 20 seconds per sending thread
            plus 10 seconds for the response queue'''
        for i in range(self.max_concurrent_batches):
            try:
                self.pending.put(None, True, 10)
            except queue.Full:
                pass
        for t in self.threads:
            t.join(10)
        # signal to the responses queue that nothing more is coming.
        try:
            self.responses.put(None, True, 10)
        except queue.Full:
            pass

    def get_response_queue(self):
        ''' return the responses queue on to which will be sent the response
        objects from each event send'''
        return self.responses

def get_now():
    return datetime.datetime.now()
