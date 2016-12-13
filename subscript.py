#import datetime
from weakref import proxy

from format import Status
from transactions import make_simple_response
from util import Loggable


class Subscription:
    def __init__(self, sub_manager, dialog):
        self.sub_manager = sub_manager
        self.dialog = dialog

        self.dialog.report_slot.plug(self.process)


    def process(self, params):
        is_response = params["is_response"]
        method = params["method"]
        
        if not is_response:
            if method == "SUBSCRIBE":
                #now = datetime.datetime.now()

                expires = params.get("expires")
                #seconds_left = int(expires) if expires is not None else 3600  # FIXME: proper default!
                #expiration = now + datetime.timedelta(seconds=seconds_left)

                res = dict(status=Status(200, "OK"), expires=expires)
                self.dialog.send_response(res, params)

                self.notify()
            else:
                self.sub_manager.logger.warning("Ignoring %s request!" % method)
        else:
            if method == "NOTIFY":
                self.sub_manager.logger.info("Got response for NOTIFY.")
            else:
                self.sub_manager.logger.warning("Ignoring %s response!" % method)
                
                
    def notify(self):
        body = b"Messages-Waiting: %s\r\n" % b"no"
        
        req = dict(
            method="NOTIFY",
            event="message-summary",
            subscription_state="active",
            content_type="application/simple-message-summary",
            body=body
        )
        
        self.dialog.send_request(req)
        

class SubscriptionManager(Loggable):
    def __init__(self, switch):
        Loggable.__init__(self)

        self.switch = switch
        self.subscriptions = []
        

    def transmit(self, params, related_params=None):
        self.switch.send_message(params, related_params)

        
    def reject_request(self, request, status):
        response = make_simple_response(request, status)
        self.transmit(response, request)


    def add_subscription(self, dialog):
        self.logger.debug("Created subscription.")
        sub = Subscription(proxy(self), dialog)
        self.subscriptions.append(sub)
            
        return sub


    def process_request(self, params):
        if params["method"] != "SUBSCRIBE":
            raise Exception("SubscriptionManager has nothing to do with this request!")
        
        if params["event"] != "message-summary":
            self.logger.warning("Ignoring subscription for unknown event type: %s!" % params["event"])
            self.reject_request(params, Status(489))
            return
            
        dialog = self.switch.make_dialog()
        self.add_subscription(dialog)
        dialog.recv_request(params)
