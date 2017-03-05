from collections import OrderedDict
import urllib.parse


ALPHANUM = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
URLLIB_SAFE = '_.-'
XML_NAME = ALPHANUM + "_.-:"


class BaseParser:
    def __init__(self, text):
        # The text should end with line terminators, so we don't have to check for length
        # And should replace all tabs with spaces, which is legal.
        self.text = text + '\n'
        self.pos = 0


    def __repr__(self):
        return "BaseParser(...%r)" % self.text[self.pos:]
        
        
    def startswith(self, what):
        return self.text[self.pos:self.pos+len(what)] == what


    def skip_whitespace(self, pos):
        while pos < len(self.text) and self.text[pos].isspace():
            pos += 1
            
        return pos
            
        
    def grab_whitespace(self):
        pos = self.skip_whitespace(self.pos)
        if pos == self.pos:
            raise Exception("Expected whitespace at %r" % self)
            
        self.pos = pos
        
        
    def can_grab_whitespace(self):
        pos = self.skip_whitespace(self.pos)
        could = pos > self.pos
        self.pos = pos
        
        return could


    def grab_number(self):
        start = self.pos
        
        while self.text[self.pos].isdigit():
            self.pos += 1
            
        end = self.pos
        number = self.text[start:end]
        
        if not number:
            raise Exception("Expected number!")
            
        return int(number)


    def grab_until(self, separator):
        start = self.pos
        end = self.text.find(separator, start)
        
        if end >= 0:
            self.pos = end
            return self.text[start:end]
        else:
            return None


    def can_grab_separator(self, wanted, left_pad=False, right_pad=False):
        pos = self.pos
        
        if left_pad:
            pos = self.skip_whitespace(pos)
            
        if pos == len(self.text) or self.text[pos] != wanted:
            return False
            
        pos += 1
        
        if right_pad:
            pos = self.skip_whitespace(pos)
            
        self.pos = pos
        
        return True


    def grab_separator(self, wanted, left_pad=False, right_pad=False):
        if not self.can_grab_separator(wanted, left_pad, right_pad):
            raise Exception("Expected separator %r at %r" % (wanted, self))
            
        
    def grab_token(self, acceptable):
        start = self.pos
        pos = start
        
        while self.text[pos] in acceptable:
            pos += 1
            
        end = pos
        token = self.text[start:end]
        self.pos = pos
        
        if not token:
            raise Exception("Expected token at %r" % self)
        
        return token
        

    def grab_quoted(self):
        pos = self.skip_whitespace(self.pos)
            
        if self.text[pos] != '"':
            raise Exception("Expected quoted-string!")
            
        pos += 1
        quoted = ""
        
        while self.text[pos] != '"':
            if self.text[pos] == '\\':
                pos += 1
                
                if self.text[pos] in "\n\r":
                    raise Exception("Illegal escaping at %r!" % self)
                
            quoted += self.text[pos]
            pos += 1
        
        pos += 1
        self.pos = self.skip_whitespace(pos)
        
        return quoted


    def grab_token_or_quoted(self, acceptable):
        pos = self.skip_whitespace(self.pos)
        
        if self.text[pos] == '"':
            return self.grab_quoted()
        else:
            return self.grab_token(acceptable)
    
    
def unescape(escaped):
    return urllib.parse.unquote(escaped)
    
    
def escape(raw, safe=''):
    # The characters not needing escaping vary from entity to entity. But characters in
    # the UNRESERVED class seems to be allowed always. That's ALPHANUM + MARK. This
    # function never quotes alphanumeric characters and '_.-', so we only need to
    # specify the rest explicitly.
    
    return urllib.parse.quote(raw, safe=safe)
    

# unquoting happens during the parsing, otherwise quoted strings can't even be parsed
def quote(raw):
    return '"' + raw.replace('\\', '\\\\').replace('"', '\\"') + '"'


def quote_unless(raw, safe):
    return raw if all(c in safe for c in raw) else quote(raw)


class Xml:
    def __init__(self, tag, attributes, content):
        self.tag = tag
        self.attributes = attributes
        self.content = content


    def __str__(self):
        if not self.tag:
            return self.content
        else:
            core = self.tag + "".join(' %s="%s"' % (n, v) for n, v in self.attributes.items())
            
            if self.content is None:
                return "<%s/>" % core
            else:
                return "<%s>%s</%s>" % (core, "".join(str(c) for c in self.content), self.tag)


    @classmethod
    def parse_element(cls, parser):
        # Grab element head
        parser.can_grab_whitespace()
        parser.grab_separator("<")
        tag = parser.grab_token(XML_NAME)
        attributes = OrderedDict()
        content = None
    
        while True:
            ws = parser.can_grab_whitespace()
        
            if parser.can_grab_separator("/"):
                # Element without content
                parser.grab_separator(">")
                break
            elif parser.can_grab_separator(">"):
                # Start element, parse children until our end tag
                content = []

                while True:
                    parser.can_grab_whitespace()
            
                    if parser.startswith("</"):
                        # End element, hopefully ours
                        break
                    elif parser.startswith("<"):
                        # Child element
                        child = cls.parse_element(parser)
                        content.append(child)
                    else:
                        # Text
                        text = parser.grab_until("<")
                
                        if not text:
                            raise Exception("Unclosed text in %s at %s!" % (tag, parser))
                    
                        child = cls(None, None, text.strip())
                        content.append(child)
                        
                parser.grab_separator("<")
                parser.grab_separator("/")
                etag = parser.grab_token(XML_NAME)
                parser.can_grab_whitespace()
                parser.grab_separator(">")
        
                if etag == tag:
                    break
                else:
                    raise Exception("Mismatching end tag %s for %s!" % (etag, tag))
            else:
                # Grab an attribute
                if not ws:
                    raise Exception("Missing whitespace before attribute at %s!" % parser)

                name = parser.grab_token(XML_NAME)
                parser.grab_separator("=", True, True)
            
                if parser.can_grab_separator("'"):
                    value = parser.grab_until("'")
                    parser.grab_separator("'")
                elif parser.can_grab_separator('"'):
                    value = parser.grab_until('"')
                    parser.grab_separator('"')
                else:
                    raise Exception("Invalid attribute value at %s!" % parser)
                
                attributes[name] = value
            
        return cls(tag, attributes, content)
        
        
    @classmethod
    def parse(cls, text):
        parser = BaseParser(text)
        parser.can_grab_whitespace()
        
        if parser.startswith("<?"):
            parser.grab_until("?>")
            parser.grab_separator("?")
            parser.grab_separator(">")
            
        return cls.parse_element(parser)
