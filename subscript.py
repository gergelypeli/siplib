from format import Status
from transactions import make_simple_response
from util import Loggable
import zap


class Subscription:
    def __init__(self, dialog):
        self.dialog = dialog
        self.expiration_plug = None
        

class EventSource(Loggable):
    MIN_EXPIRES = 30
    MAX_EXPIRES = 60
    
    def __init__(self):
        Loggable.__init__(self)
        
        self.subscriptions_by_id = {}
        self.last_subscription_id = 0


    def add_subscription(self, dialog):
        self.last_subscription_id += 1
        id = self.last_subscription_id
        self.logger.info("Adding subscription %s." % id)
        
        self.subscriptions_by_id[id] = Subscription(dialog)
        dialog.report_slot.plug(self.process, id=id)


    def process(self, params, id):
        is_response = params["is_response"]
        method = params["method"]
        
        if not is_response:
            if method == "SUBSCRIBE":
                self.logger.info("Got SUBSCRIBE request.")
                subscription = self.subscriptions_by_id[id]

                expires = params.get("expires", 0)
                if expires > 0:
                    if expires < self.MIN_EXPIRES:
                        res = dict(status=Status(423), expires=self.MIN_EXPIRES)
                        subscription.dialog.send_response(res, params)
                    
                        if not subscription.expiration_plug:
                            self.subscriptions_by_id.pop(id)
                        
                        return
                    
                    if expires > self.MAX_EXPIRES:
                        expires = self.MAX_EXPIRES
                    
                    if subscription.expiration_plug:
                        subscription.expiration_plug.unplug()
                    
                    subscription.expiration_plug = zap.time_slot(expires).plug(self.expired, id=id)

                res = dict(status=Status(200, "OK"), expires=expires)
                subscription.dialog.send_response(res, params)

                if expires > 0:
                    self.logger.info("Subscribed for %d seconds." % expires)
                    self.notify_one(id)
                else:
                    self.logger.info("Unsubscribed.")
                    self.notify_one(id, "timeout")
                    self.subscriptions_by_id.pop(id, None)
            else:
                self.logger.warning("Ignoring %s request!" % method)
        else:
            if method == "NOTIFY":
                self.logger.info("Got NOTIFY response.")
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
        
        # Must make copies for multiple notifications
        req = dict(
            state,
            method="NOTIFY",
            subscription_state="active" if not reason else "terminated;reason=%s" % reason
        )
        
        subscription.dialog.send_request(req)
        
        
    def notify_all(self, reason=None):
        self.logger.info("Notifying all subscriptions.")
        state = self.get_state()
        
        for id in self.subscriptions_by_id:
            self.notify_one(id, reason, state)
            
                
    def expired(self, id):
        self.logger.info("Expired subscription %s." % id)
        self.notify_one(id, "timeout")
        self.subscriptions_by_id.pop(id, None)


class SubscriptionManager(Loggable):
    def __init__(self, switch):
        Loggable.__init__(self)

        self.switch = switch
        self.event_sources_by_id = {}
        

    def transmit(self, params, related_params=None):
        # For out of dialog responses
        self.switch.send_message(params, related_params)

        
    def reject_request(self, request, status):
        response = make_simple_response(request, status)
        self.transmit(response, request)


    def identify_event_source(self, params):
        raise NotImplementedError()


    def make_event_source(self, type, label):
        raise NotImplementedError()


    def get_event_source(self, type, label):
        es = self.event_sources_by_id.get((type, label))
        
        if not es:
            self.logger.debug("Creating event source: %s=%s." % (type, label))
            es = self.make_event_source(type, label)
            if not es:
                raise Exception("Couldn't make event source of type %s!" % type)
                
            es.set_oid(self.oid.add(type, label))
            self.event_sources_by_id[(type, label)] = es
            
        return es


    def process_request(self, params):
        if params["method"] != "SUBSCRIBE":
            raise Exception("SubscriptionManager has nothing to do with this request!")
        
        type, label = self.identify_event_source(params) or (None, None)
        
        if not type:
            self.logger.warning("Ignoring subscription for unknown event source!")
            self.reject_request(params, Status(489))
            return
        
        es = self.get_event_source(type, label)
        dialog = self.switch.make_dialog()
        es.add_subscription(dialog)
        dialog.recv_request(params)
