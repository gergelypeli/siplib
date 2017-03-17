import datetime
from weakref import proxy

from format import Status, Sip, Nameaddr
from transactions import make_simple_response
from log import Loggable
from zap import Plug, EventSlot
from util import generate_call_id, generate_tag, MAX_FORWARDS


# The Snom seems to miss the expiration of 60 seconds for some reason.
MIN_EXPIRES_SECONDS = 60
MAX_EXPIRES_SECONDS = 300


class UnsolicitedDialog:
    def __init__(self, manager, local_uri, remote_uri, hop):
        self.manager = manager
        self.local_uri = local_uri
        self.remote_uri = remote_uri
        self.hop = hop
        
        self.call_id = generate_call_id()
        self.last_sent_cseq = 0
        self.message_slot = EventSlot()  # not used
        
        
    def send(self, msg):
        if msg.is_response or msg.method != "NOTIFY":
            raise Exception("WTF?")
            
        request = msg
        self.last_sent_cseq += 1
        
        request.uri = self.remote_uri
        request.hop = self.hop
        request["from"] = Nameaddr(self.local_uri, params=dict(tag=generate_tag()))
        request["to"] = Nameaddr(self.remote_uri)
        request["call_id"] = self.call_id
        request["cseq"] = self.last_sent_cseq
        request["max_forwards"] = MAX_FORWARDS
        
        self.manager.send_message(msg)


class Subscription:
    def __init__(self, format, dialog):
        self.format = format
        self.dialog = dialog
        self.expiration_deadline = None
        self.expiration_plug = None
        

class EventSource(Loggable):
    def __init__(self, formats):
        Loggable.__init__(self)
        
        self.subscriptions_by_id = {}
        self.last_subscription_id = 0
        self.formats = formats
        self.state = None


    def identify(self, params):
        raise NotImplementedError()
        

    def get_state(self, format):
        raise NotImplementedError()

        
    def set_state(self, state):
        self.state = state
        self.notify_all()
        

    def get_expiry_range(self):
        return MIN_EXPIRES_SECONDS, MAX_EXPIRES_SECONDS


    def add_subscription(self, format, dialog):
        if format not in self.formats:
            raise Exception("No formatter for format: %s!" % format)
            
        self.last_subscription_id += 1
        id = self.last_subscription_id
        self.logger.info("Adding subscription %s." % id)
        
        s = Subscription(format, dialog)
        self.subscriptions_by_id[id] = s
        
        s.expiration_plug = Plug(self.subscription_expired, id=id)
        Plug(self.process, id=id).attach(dialog.message_slot)
        
        return id


    def process(self, msg, id):
        if not msg.is_response:
            request = msg
            method = request.method
            # NOTE: we can only handle one subscription per dialog, sharing is not supported.
        
            if method == "SUBSCRIBE":
                self.logger.debug("Got SUBSCRIBE request.")
                subscription = self.subscriptions_by_id[id]

                expires = request.get("expires", 0)
                min_expires, max_expires = self.get_expiry_range()
            
                if expires > 0:
                    if expires < min_expires:
                        res = Sip.response(status=Status.INTERVAL_TOO_BRIEF, expires=min_expires)
                        subscription.dialog.send(res, request)
                
                        if not subscription.expiration_plug:
                            self.subscriptions_by_id.pop(id)
                    
                        return
                
                    if expires > max_expires:
                        expires = max_expires
                
                    subscription.expiration_plug.detach()
                
                    subscription.expiration_deadline = datetime.datetime.now() + datetime.timedelta(seconds=expires)
                    subscription.expiration_plug.attach_time(expires)
                
                    res = Sip.response(status=Status.OK, related=request)
                    res["expires"] = expires
                    subscription.dialog.send(res)

                    # FIXME
                    contact = subscription.dialog.peer_contact.uri
                    self.logger.info("Subscription %s from %s extended for %d seconds." % (id, contact, expires))
                    self.notify_one(id)
                else:
                    if subscription.expiration_plug:
                        self.logger.info("Subscription %s cancelled." % (id,))
                        subscription.expiration_plug.detach()
                    else:
                        self.logger.info("Subscription %s polled." % (id,))

                    res = Sip.response(status=Status.OK, related=request)
                    res["expires"] = expires
                    subscription.dialog.send(res)

                    self.notify_one(id, "timeout")
                    self.subscriptions_by_id.pop(id, None)
            else:
                self.logger.warning("Ignoring %s request!" % method)

        else:
            response = msg
            method = response.method
        
            if method == "NOTIFY":
                status = response.status
            
                if status.code == 200:
                    self.logger.debug("Got NOTIFY response.")
                else:
                    self.logger.warning("Got NOTIFY response %d!" % status.code)
                    s = self.subscriptions_by_id.pop(id, None)
                    
                    if s:
                        s.expiration_plug.detach()
            else:
                self.logger.warning("Ignoring %s response!" % method)


    def send_notify(self, id, state_by_format, reason=None):
        subscription = self.subscriptions_by_id[id]
        
        event, content_type, body = state_by_format[subscription.format]
        
        if reason:
            ss = "terminated;reason=%s" % reason
        elif subscription.expiration_deadline:
            expires = subscription.expiration_deadline - datetime.datetime.now()
            ss = "active;expires=%d" % expires.total_seconds()
        else:
            ss = "active"
            
        # Must make copies in case of multiple subscriptions
        req = Sip.request(method="NOTIFY")
        req["subscription_state"] = ss
        req["event"] = event
        req["content_type"] = content_type
        req.body = body
        
        subscription.dialog.send(req)


    def notify_one(self, id, reason=None):
        self.logger.info("Notifying subscription %s." % id)
        
        subscription = self.subscriptions_by_id[id]
        f = subscription.format
        state_by_format = { f: self.get_state(f) }

        self.send_notify(id, state_by_format, reason)

        
    def notify_all(self, reason=None):
        self.logger.info("Notifying all subscriptions.")
        
        state_by_format = { f: self.get_state(f) for f in self.formats }
        
        for id in self.subscriptions_by_id:
            self.send_notify(id, state_by_format, reason)
            
                
    def subscription_expired(self, id):
        self.logger.info("Expired subscription %s." % id)
        self.notify_one(id, "timeout")
        self.subscriptions_by_id.pop(id)


class EventFormatter:
    def format(self, state):
        raise NotImplementedError()
        

class MessageSummaryFormatter(EventFormatter):
    # RFC 3458
    MESSAGE_CONTEXT_CLASSES = [ "voice", "fax", "pager", "multimedia", "text" ]
    

    def format(self, ms):
        waiting = "yes" if any(ms.values()) else "no"
        text = "Messages-Waiting: %s\r\n" % waiting

        for mcc in self.MESSAGE_CONTEXT_CLASSES:
            new = ms.get(mcc, 0)
            old = ms.get("%s_old" % mcc, 0)
            
            if new or old:
                text += "%s-Message: %d/%d\r\n" % (mcc.title(), new, old)
        
        event = "message-summary"
        content_type = "application/simple-message-summary"
        body = text.encode("utf8")
        
        return event, content_type, body


class DialogFormatter(EventFormatter):
    def __init__(self):
        self.entity = None
        self.version = 0
        
        
    def set_entity(self, entity):
        self.entity = entity
        
        
    def side_lines(self, side, info):
        lines = []
        side_info = info.get(side)

        if side_info is not None:
            lines.append('<%s>' % side)
        
            identity = side_info.get("identity")  # nameaddr
            if identity:
                lines.append('<identity display="%s">%s</identity>' % (identity.name, identity.uri))
        
            target = side_info.get("target")  # nameaddr
            if target:
                lines.append('<target uri="%s"/>' % (target.uri,))

            # TODO: target.params are treated as Contact header parameters,
            # and can be included in <param/> sub-elements.
            
            lines.append('</%s>' % side)
            
        return lines

        
    def format(self, ds):
        lines = []
        
        lines.append('<?xml version="1.0"?>')
        
        lines.append('<dialog-info xmlns="urn:ietf:params:xml:ns:dialog-info" version="%d" state="full" entity="%s">' % (self.version, self.entity))
        self.version += 1
        
        for id, info in ds.items():
            is_outgoing, is_confirmed = info["is_outgoing"], info["is_confirmed"]
            call_id, local_tag, remote_tag = (info.get(x) for x in ("call_id", "local_tag", "remote_tag"))
            
            attrs = [
                'id="%s"' % id,
                'direction="%s"' % ("initiator" if is_outgoing else "recipient"),
                'call-id="%s"' % call_id if call_id else None,
                'local-tag="%s"' % local_tag if local_tag else None,
                'remote-tag="%s"' % remote_tag if remote_tag else None
            ]
            lines.append('<dialog %s>' % " ".join(a for a in attrs if a))
            
            state = "confirmed" if is_confirmed else "early"
            lines.append('<state>%s</state>' % state)
            
            lines.extend(self.side_lines("local", info))
            lines.extend(self.side_lines("remote", info))
            
            lines.append('</dialog>')
            
        lines.append('</dialog-info>')

        event = "dialog"
        content_type = "application/dialog-info+xml"
        body = "\n".join(lines).encode("utf8")
        
        return event, content_type, body


class PresenceFormatter(EventFormatter):
    XML = """
<presence entity="%s" xmlns="urn:ietf:params:xml:ns:pidf" xmlns:pp="urn:ietf:params:xml:ns:pidf:person" xmlns:ep="urn:ietf:params:xml:ns:pidf:rpid:rpid-person">
    <tuple id="siplib">
        <status>
            <basic>%s</basic>
        </status>
    </tuple>
    <pp:person>
        <status>
            <ep:activities>
                %s
            </ep:activities>
        </status>
    </pp:person>
    <note>
        %s
    </note>
</presence>
"""

    def __init__(self):
        self.entity = None
        
        
    def set_entity(self, entity):
        self.entity = entity


    def format(self, state):
        basic = "open" if state.get("is_open") else "closed"
        activities = ""
        note = ""
        
        if state.get("is_ringing"):
            note = "Ringing"
        
        if state.get("is_busy"):
            activities += "<ep:busy/>"
        
        if state.get("is_dnd"):
            activities += "<ep:away/>"
        
        xml = self.XML % (self.entity, basic, activities, note)
        
        event = "presence"
        content_type = "application/pidf+xml"
        body = xml.encode("utf8")
        
        return event, content_type, body


class CiscoPresenceFormatter(PresenceFormatter):
    XML = """
<presence entity="%s" xmlns="urn:ietf:params:xml:ns:pidf" xmlns:dm="urn:ietf:params:xml:ns:pidf:data-model" xmlns:e="urn:ietf:params:xml:ns:pidf:status:rpid" xmlns:ce="urn:cisco:params:xml:ns:pidf:rpid">
    <tuple id="siplib">
        <status>
            <basic>%s</basic>
        </status>
    </tuple>
    <dm:person>
        <e:activities>
            %s
        </e:activities>
    </dm:person>
</presence>
"""

    def format(self, state):
        basic = "open" if state["is_open"] else "closed"
        activities = ""
        
        if state.get("is_ringing"):
            activities += "<ce:alerting/>"
            
        if state.get("is_busy"):
            activities += "<e:on-the-phone/>"
            
        if state.get("is_dnd"):
            activities += "<ce:dnd/>"
            
        if not activities:
            activities = "<ce:available/>"
            
        xml = self.XML % (self.entity, basic, activities)

        event = "presence"
        content_type = "application/pidf+xml"
        body = xml.encode("utf8")
        
        return event, content_type, body
        

class SubscriptionManager(Loggable):
    def __init__(self, switch):
        Loggable.__init__(self)

        self.switch = switch
        self.event_sources_by_key = {}
        

    def send_message(self, msg):
        # For out of dialog responses and unsolicited stuff
        self.switch.send_message(msg)

        
    def reject_request(self, request, status):
        response = make_simple_response(request, status)
        self.send_message(response)


    def identify_subscription(self, request):
        raise NotImplementedError()


    def make_event_source(self, type):
        raise NotImplementedError()


    def add_event_source(self, type, params):
        es = self.make_event_source(type)
        id = es.identify(params)
        self.logger.debug("Created event source: %s=%s." % (type, id))

        es.set_oid(self.oid.add(type, id))
        key = (type, id)
        self.event_sources_by_key[key] = es
        

    def process(self, msg):
        if msg.is_response:
            self.logger.warning("Ignoring response!")
            return
            
        request = msg
        if request.method != "SUBSCRIBE":
            raise Exception("SubscriptionManager has nothing to do with this request!")
        
        iden = self.identify_subscription(request)
        
        if not iden:
            self.logger.warning("Rejecting subscription for unidentifiable event source!")
            self.reject_request(request, Status.NOT_FOUND)
            return
        
        type, id, format = iden
        key = type, id
        
        es = self.event_sources_by_key.get(key)
        if not es:
            self.logger.warning("Rejecting subscription for nonexistent event source: %s" % (key,))
            self.reject_request(request, Status.NOT_FOUND)
            return
            
        dialog = self.switch.make_dialog()
        es.add_subscription(format, dialog)
        dialog.recv(request)


    def unsolicited_subscribe(self, es_type, es_id, format, local_uri, remote_uri, hop):
        key = es_type, es_id
        es = self.event_sources_by_key.get(key)
        
        if not es:
            self.logger.warning("Ignoring subscription for nonexistent event source: %s" % (key,))
            return
        
        self.logger.info("Will send unsolicited notifications for %s from %s to %s via %s." % (key, local_uri, remote_uri, hop))
        dialog = UnsolicitedDialog(proxy(self), local_uri, remote_uri, hop)
        id = es.add_subscription(format, dialog)
        es.notify_one(id)
        
        return id
        
        
    def unsolicited_unsubscribe(self, es_type, es_id, id):
        key = es_type, es_id
        es = self.event_sources_by_key.get(key)
        
        if not es:
            self.logger.warning("Ignoring unsubscription for nonexistent event source: %s" % (key,))
            return

        es.subscription_expired(id)


    def get_event_source(self, type, id):
        key = (type, id)
        return self.event_sources_by_key.get(key)
