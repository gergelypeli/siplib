import datetime

from format import Status, Sip
from transactions import make_simple_response
from log import Loggable
from zap import Plug


class Subscription:
    def __init__(self, dialog):
        self.dialog = dialog
        self.expiration_deadline = None
        self.expiration_plug = None
        

class EventSource(Loggable):
    def __init__(self):
        Loggable.__init__(self)
        
        self.subscriptions_by_id = {}
        self.last_subscription_id = 0


    def identify(self, params):
        raise NotImplementedError()
        

    def get_expiry_range(self):
        return 30, 60


    def add_subscription(self, dialog):
        self.last_subscription_id += 1
        id = self.last_subscription_id
        self.logger.info("Adding subscription %s." % id)
        
        s = Subscription(dialog)
        self.subscriptions_by_id[id] = s
        
        s.expiration_plug = Plug(self.expired, id=id)
        Plug(self.process, id=id).attach(dialog.message_slot)


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
                    self.subscriptions_by_id.pop(id, None)
            else:
                self.logger.warning("Ignoring %s response!" % method)


    def get_state(self):
        raise NotImplementedError()


    def notify_one(self, id, reason=None, state=None):
        subscription = self.subscriptions_by_id[id]
        
        if not state:
            # So this is not a bulk notification
            self.logger.info("Notifying subscription %s." % id)
            state = self.get_state()
        
        if not reason:
            expires = subscription.expiration_deadline - datetime.datetime.now()
            ss = "active;expires=%d" % expires.total_seconds()
        else:
            ss = "terminated;reason=%s" % reason
            
        # Must make copies in case of multiple subscriptions
        req = Sip.request(method="NOTIFY")
        req["subscription_state"] = ss
        req.body = state.pop("body", None)
        req.update(state)
        
        subscription.dialog.send(req)
        
        
    def notify_all(self, reason=None):
        self.logger.info("Notifying all subscriptions.")
        state = self.get_state()
        
        for id in self.subscriptions_by_id:
            self.notify_one(id, reason, state)
            
                
    def expired(self, id):
        self.logger.info("Expired subscription %s." % id)
        self.notify_one(id, "timeout")
        self.subscriptions_by_id.pop(id)


class MessageSummaryEventSource(EventSource):
    # RFC 3458
    MESSAGE_CONTEXT_CLASSES = [ "voice", "fax", "pager", "multimedia", "text" ]
    
    def get_message_state(self):
        raise NotImplementedError()
        
        
    def get_state(self):
        ms = self.get_message_state()
        
        waiting = "yes" if any(ms.values()) else "no"
        body = "Messages-Waiting: %s\r\n" % waiting

        for mcc in self.MESSAGE_CONTEXT_CLASSES:
            new = ms.get(mcc, 0)
            old = ms.get("%s_old" % mcc, 0)
            
            if new or old:
                body += "%s-Message: %d/%d\r\n" % (mcc.title(), new, old)
        
        return dict(
            event="message-summary",
            content_type="application/simple-message-summary",
            body=body.encode("utf8")
        )


class DialogEventSource(EventSource):
    def __init__(self):
        EventSource.__init__(self)
        
        self.version = 0
        self.entity = None
        
        
    def set_entity(self, entity):
        self.entity = entity
        
        
    def get_dialog_state(self):
        raise NotImplementedError()


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

        
    def get_state(self):
        ds = self.get_dialog_state()
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

        return dict(
            event="dialog",
            content_type="application/dialog-info+xml",
            body="\n".join(lines).encode("utf8")
        )


class SubscriptionManager(Loggable):
    def __init__(self, switch):
        Loggable.__init__(self)

        self.switch = switch
        self.event_sources_by_key = {}
        

    def transmit(self, msg):
        # For out of dialog responses
        self.switch.send_message(msg)

        
    def reject_request(self, request, status):
        response = make_simple_response(request, status)
        self.transmit(response)


    def identify_event_source(self, request):
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
        
        key = self.identify_event_source(request)
        
        if not key:
            self.logger.warning("Rejecting subscription for unidentifiable event source!")
            self.reject_request(request, Status.NOT_FOUND)
            return
        
        es = self.event_sources_by_key.get(key)
        if not es:
            self.logger.warning("Rejecting subscription for nonexistent event source: %s" % (key,))
            self.reject_request(request, Status.NOT_FOUND)
            return
            
        dialog = self.switch.make_dialog()
        es.add_subscription(dialog)
        dialog.recv(request)


    def get_event_source(self, type, id):
        key = (type, id)
        return self.event_sources_by_key.get(key)
