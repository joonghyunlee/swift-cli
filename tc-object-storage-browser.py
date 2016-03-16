#!/usr/bin/python
# -*- coding: utf8 -*-

import itertools
import re
import os
import sys

import urwid

import json
import requests
import ConfigParser

class TCObjectStorageClient:
    def __init__(self, config_filename='../setup.ini'):
        self.conf = ConfigParser.ConfigParser()
        self.conf.read(config_filename)
        
        self.project_id = self.conf.get('default', 'project_id')

        self.keystone_endpoint = self.conf.get('object_storage', 'keystone_endpoint')
        self.swift_endpoint = self.conf.get('object_storage', 'object_storage_endpoint')    

        self.username = self.conf.get('default', 'username')
        self.password = self.conf.get('object_storage', 'password')
        
        self.headers = dict()   
        self.headers['Content-Type'] = 'application/json; charset=utf-8'
        
        token, self.tenant_id = self._get_token()
        self.headers['X-Auth-Token'] = token
        print token

    def _get_token(self):
        request = dict()
        auth = dict()
        passwordCredentials = dict()
        passwordCredentials['username'] = self.username
        passwordCredentials['password'] = self.password
        auth['passwordCredentials'] = passwordCredentials
        auth['tenantName'] = self.project_id
        request['auth'] = auth

        url = self.keystone_endpoint + '/identity/v2.0/tokens'
        response = requests.post(url, data=json.dumps(request), headers=self.headers)

        response_body = response.json()
        token = response_body['access']['token']['id'].encode('utf-8')
        tenant_id = response_body['access']['token']['tenant']['id'].encode('utf-8')

        return token, tenant_id

    def get_containers(self):
        url = self.swift_endpoint + '/v1/AUTH_' + self.tenant_id

        response = requests.get(url, headers=self.headers)
        
        """ If the request is failed, it raise HTTPError exceptions"""
        """ If the status code is 200, then it does nothing"""
        response.raise_for_status()

        """ Convert plain text to container list """
        containers = response.text.encode('utf-8').split('\n')
        containers.remove('')

        return containers

    def get_objects(self, container):
        url = self.swift_endpoint + '/v1/AUTH_' + self.tenant_id
        url = url + '/' + container

        response = requests.get(url, headers=self.headers)

        """ If the request is failed, it raise HTTPError exceptions"""
        """ If the status code is 200, then it does nothing"""
        response.raise_for_status()

        """ Convert plain text to object list """
        objects = response.text.encode('utf-8').split('\n')
        objects.remove('')

        return objects

    def get_object_metadata(self, container, object_name):
        url = self.swift_endpoint + '/v1/AUTH_' + self.tenant_id
        url = url + container  + '/' + object_name

        params = dict()
        params['format'] = 'json'

        response = requests.head(url, headers=self.headers, params=params)
        
        """ If the request is failed, it raise HTTPError exceptions """
        """ If the status code is 200, then it does nothing """
        response.raise_for_status()

        """ Check whether the Object is folder or file """
        is_folder = True if response.headers['Content-Length'] is 0 else False

        return is_folder, response.headers

    def get_objects_in_pseudo_folder(self, container, prefix):
        url = self.swift_endpoint + '/v1/AUTH_' + self.tenant_id
        url = url + '/' + container

        params = dict()
        params['prefix'] = prefix
        params['delimiter'] = '/'

        response = requests.get(url, headers=self.headers, params=params)

        """ If the request is failed, it raise HTTPError exceptions"""
        """ If the status code is 200, then it does nothing"""
        response.raise_for_status()

        """ Convert plain text to object list """
        objects = response.text.encode('utf-8').split('\n')
        objects.remove('')
        for each in objects:
            if each == prefix:
                objects.remove(each)

        return objects

    def upload_object(self, path, filename):
        url = self.swift_endpoint + '/v1/AUTH_' + self.tenant_id
        url = url + path + filename

        fp = open(filename, 'r')
        headers = self.headers
        headers['Content-Type'] = 'multipart/formed-data'

        response = requests.put(url, headers=headers, files={'file':fp})
        print response.status_code
        print response.headers

    def get_container_metadata(self, container):
        url = self.swift_endpoint + '/v1/AUTH_' + self.tenant_id
        url = url + container
        response = requests.head(url, headers=self.headers)

        """ If the request is failed, it raise HTTPError exceptions"""
        """ If the status code is 200, then it does nothing"""
        response.raise_for_status()

        return response.headers

    def create_container(self, container):
        url = self.swift_endpoint + '/v1/AUTH_' + self.tenant_id
        url = url + container
        response = requests.put(url, headers=self.headers)

        """ If the request is failed, it raise HTTPError exceptions"""
        """ If the status code is 200, then it does nothing"""
        response.raise_for_status()

    def delete_container(self, container):
        url = self.swift_endpoint + '/v1/AUTH_' + self.tenant_id
        url = url + container
        response = requests.delete(url, headers=self.headers)

        """ If the request is failed, it raise HTTPError exceptions"""
        """ If the status code is 200, then it does nothing"""
        response.raise_for_status()

objectstorageclient = TCObjectStorageClient()

class FlagFileWidget(urwid.TreeWidget):
    # apply an attribute to the expand/unexpand icons
    unexpanded_icon = urwid.AttrMap(urwid.TreeWidget.unexpanded_icon,
        'dirmark')
    expanded_icon = urwid.AttrMap(urwid.TreeWidget.expanded_icon,
        'dirmark')

    def __init__(self, node):
        self.__super.__init__(node)
        # insert an extra AttrWrap for our own use
        self._w = urwid.AttrWrap(self._w, None)
        self.flagged = False
        self.update_w()

    def selectable(self):
        return True

    def keypress(self, size, key):
        """allow subclasses to intercept keystrokes"""
        key = self.__super.keypress(size, key)
        if key:
            key = self.unhandled_keys(size, key)
        return key

    def unhandled_keys(self, size, key):
        """
        Override this method to intercept keystrokes in subclasses.
        Default behavior: Toggle flagged on space, ignore other keys.
        """
        if key == " ":
            self.flagged = not self.flagged
            self.update_w()
        else:
            return key

    def update_w(self):
        """Update the attributes of self.widget based on self.flagged.
        """
        if self.flagged:
            self._w.attr = 'flagged'
            self._w.focus_attr = 'flagged focus'
        else:
            self._w.attr = 'body'
            self._w.focus_attr = 'focus'


class FileTreeWidget(FlagFileWidget):
    """Widget for individual files."""
    def __init__(self, node):
        self.__super.__init__(node)
        path = node.get_value()
        add_widget(path, self)

    def get_display_text(self):
        return self.get_node().get_key()



class EmptyWidget(urwid.TreeWidget):
    """A marker for expanded directories with no contents."""
    def get_display_text(self):
        return ('flag', '(empty directory)')


class ErrorWidget(urwid.TreeWidget):
    """A marker for errors reading directories."""

    def get_display_text(self):
        return ('error', "(error/permission denied)")


class DirectoryWidget(FlagFileWidget):
    """Widget for a directory."""
    def __init__(self, node):
        self.__super.__init__(node)
        path = node.get_value()
        add_widget(path, self)
        self.expanded = starts_expanded(path)
        self.update_expanded_icon()

    def get_display_text(self):
        node = self.get_node()
        if node.get_depth() == 0:
            return "/"
        else:
            return node.get_key()


class FileNode(urwid.TreeNode):
    """Metadata storage for individual files"""

    def __init__(self, path, parent=None):
        depth = path.count(dir_sep())
        key = os.path.basename(path)
        urwid.TreeNode.__init__(self, path, key=key, parent=parent, depth=depth)

    def load_parent(self):
        parentname, myname = os.path.split(self.get_value())
        parent = DirectoryNode(parentname)
        parent.set_child_node(self.get_key(), self)
        return parent

    def load_widget(self):
        return FileTreeWidget(self)


class EmptyNode(urwid.TreeNode):
    def load_widget(self):
        return EmptyWidget(self)


class ErrorNode(urwid.TreeNode):
    def load_widget(self):
        return ErrorWidget(self)


class DirectoryNode(urwid.ParentNode):
    """Metadata storage for directories"""

    def __init__(self, path, parent=None):
        if path == dir_sep():
            depth = 0
            key = None
        else:
            depth = path.count(dir_sep())
            key = os.path.basename(path)
        urwid.ParentNode.__init__(self, path, key=key, parent=parent,
                                  depth=depth)

    def load_parent(self):
        parentname, myname = os.path.split(self.get_value())
        parent = DirectoryNode(parentname)
        parent.set_child_node(self.get_key(), self)
        return parent

    def load_child_keys(self):
        dirs = []
        files = []
        try:
            path = self.get_value()
            # separate dirs and files
            for a in os.listdir(path):
                if os.path.isdir(os.path.join(path,a)):
                    dirs.append(a)
                else:
                    files.append(a)
        except OSError, e:
            depth = self.get_depth() + 1
            self._children[None] = ErrorNode(self, parent=self, key=None,
                                             depth=depth)
            return [None]

        # sort dirs and files
        dirs.sort(key=alphabetize)
        files.sort(key=alphabetize)
        # store where the first file starts
        self.dir_count = len(dirs)
        # collect dirs and files together again
        keys = dirs + files
        if len(keys) == 0:
            depth=self.get_depth() + 1
            self._children[None] = EmptyNode(self, parent=self, key=None,
                                             depth=depth)
            keys = [None]
        return keys

    def load_child_node(self, key):
        """Return either a FileNode or DirectoryNode"""
        index = self.get_child_index(key)
        if key is None:
            return EmptyNode(None)
        else:
            path = os.path.join(self.get_value(), key)
            if index < self.dir_count:
                return DirectoryNode(path, parent=self)
            else:
                path = os.path.join(self.get_value(), key)
                return FileNode(path, parent=self)

    def load_widget(self):
        return DirectoryWidget(self)

class ObjectNode(urwid.TreeNode):
    def __init__(self, path, parent, depth):
        key = path.split('/')[-1]
        urwid.TreeNode.__init__(self, path, key=key, parent=parent, depth=depth)

    def load_parent(self):
        return self.get_parent()

    def load_widget(self):
        return FileTreeWidget(self)

class FolderNode(urwid.ParentNode):
    def __init__(self, value, key, depth, parent):
        urwid.ParentNode.__init__(self, value=value, key=key, parent=parent, depth=depth)

    def load_parent(self):
        return self.get_parent()

    def load_child_keys(self):
        container = self.get_value().split('/', 1)[0]
        prefix = self.get_value().split('/', 1)[1]
        keys = objectstorageclient.get_objects_in_pseudo_folder(container, prefix)

        if len(keys) == 0:
            depth = self.get_depth() + 1
            self._children[None] = EmptyNode(self, parent=self, key=None, depth=depth)
            keys = [None]
            return keys

        converted_keys = []
        for key in keys:
            postfix = key.split('/', self.get_depth()-1)[-1]

            is_folder = bool(re.search('.*\/', postfix))
            if is_folder is True:
                key = postfix.split('/', 1)[0] + '/'
            else:
                key = postfix
            
            if key not in converted_keys:
                converted_keys.append(key)

        return converted_keys

    def load_child_node(self, key):
        if key is None:
            return EmptyNode(None)

        if len(key) == 0:
            return EmptyNode(None)

        is_folder = bool(re.search('.*\/', key))
        if is_folder is True:
            path = self.get_value() + key
            return FolderNode(path, key, parent=self, depth=self.get_depth()+1)
        else:
            return ObjectNode(key, parent=self, depth=self.get_depth()+1)

    def load_widget(self):
        return DirectoryWidget(self)

class ContainerNode(urwid.ParentNode):
    def __init__(self, name, parent):
        depth = 1
        path = name
        urwid.ParentNode.__init__(self, value=path, key=name, parent=parent, depth=depth)

    def load_parent(self):
        return self.get_parent()

    def load_child_keys(self):
        keys = objectstorageclient.get_objects(self.get_key())

        if len(keys) == 0:
            depth = self.get_depth() + 1
            self._children[None] = EmptyNode(self, parent=self, key=None, depth=depth)
            keys = [None]
            return keys

        converted_keys = []
        for key in keys:
            #is_folder = '/' in key
            is_folder = bool(re.search('.*\/', key))
            if is_folder is True:
                """ Example) key = folder1/folder2/.../object """
                """ name = folder1/ """
                key = key.split('/', 1)[0] + '/'

            if key not in converted_keys:
                converted_keys.append(key)

        return converted_keys

    def load_child_node(self, key):
        if key is None:
            return EmptyNode(None)

        if len(key) == 0:
            return EmptyNode(None)

        """ If a object has '/' characters, it is regarded as folder """
        #is_folder = '/' in key
        is_folder = bool(re.search('.*\/', key))

        if is_folder is True:
            """ Example) key = folder1/folder2/.../object """
            """ name = folder1 """
            """ path = container/folder1 """
            path = self.get_value() + '/' + key
            return FolderNode(value=path, key=key, parent=self, depth=self.get_depth()+1)
        else:
            return ObjectNode(key, parent=self, depth=self.get_depth()+1)

    def load_widget(self):
        return DirectoryWidget(self)
 
class AccountNode(urwid.ParentNode):
    def __init__(self):
        depth = 0
        path = '/'
        urwid.ParentNode.__init__(self, value=path, key=path, depth=depth)

    def load_child_keys(self):
        keys = objectstorageclient.get_containers()
        return keys

    def load_child_node(self, key):
        if key is None:
            return EmptyNode(None)

        return ContainerNode(key, parent=self)

    def load_widget(self):
        return DirectoryWidget(self)
          
class DirectoryBrowser:
    palette = [
        ('body', 'black', 'light gray'),
        ('flagged', 'black', 'dark green', ('bold','underline')),
        ('focus', 'light gray', 'dark blue', 'standout'),
        ('flagged focus', 'yellow', 'dark cyan',
                ('bold','standout','underline')),
        ('head', 'yellow', 'black', 'standout'),
        ('foot', 'light gray', 'black'),
        ('key', 'light cyan', 'black','underline'),
        ('title', 'white', 'black', 'bold'),
        ('dirmark', 'black', 'dark cyan', 'bold'),
        ('flag', 'dark gray', 'light gray'),
        ('error', 'dark red', 'light gray'),
        ]

    footer_text = [
        ('title', "Directory Browser"), "    ",
        ('key', "UP"), ",", ('key', "DOWN"), ",",
        ('key', "PAGE UP"), ",", ('key', "PAGE DOWN"),
        "  ",
        ('key', "SPACE"), "  ",
        ('key', "+"), ",",
        ('key', "-"), "  ",
        ('key', "LEFT"), "  ",
        ('key', "HOME"), "  ",
        ('key', "END"), "  ",
        ('key', "TAB"), "  ",
        ('key', "Q"),
        ]


    def __init__(self):
        """Get TOAST Cloud Object Storage information"""
        #self.object_storage = TCObjectStorageClient()
        #self.object_storage.get_objects('/')

        """Get local filesystem information"""
        cwd = os.getcwd()
        store_initial_cwd(cwd)
        self.header = urwid.Text("")
        self.left_listbox = urwid.TreeListBox(urwid.TreeWalker(DirectoryNode(cwd)))
        self.left_listbox.offset_rows = 1

        #self.right_listbox = urwid.TreeListBox(urwid.TreeWalker(DirectoryNode(cwd)))
        self.right_listbox = urwid.TreeListBox(urwid.TreeWalker(AccountNode()))
        self.right_listbox.offset_rows = 1
        self.listbox = urwid.Columns([
                        self.left_listbox,
                        self.right_listbox
                        ])
        self.footer = urwid.AttrWrap(urwid.Text(self.footer_text), 'foot')
        self.view = urwid.Frame(
            urwid.AttrWrap(self.listbox, 'body'),
            header=urwid.AttrWrap(self.header, 'head'),
            footer=self.footer)

    def main(self):
        """Run the program."""

        self.loop = urwid.MainLoop(self.view, self.palette,
            unhandled_input=self.unhandled_input)
        self.loop.run()

        # on exit, write the flagged filenames to the console
        names = [escape_filename_sh(x) for x in get_flagged_names()]
        print " ".join(names)

    def unhandled_input(self, k):
        # update display of focus directory
        if k in ('q','Q'):
            raise urwid.ExitMainLoop()
        elif k in ('u', 'U'):
            focus_column = self.listbox.get_focus_column()
            if focus_column == 0:
                file_focus_widget, file_focus_position = self.left_listbox.get_focus()
                object_focus_widget, object_focus_position = self.right_listbox.get_focus()
                print file_focus_widget.get_node().get_value()
                print object_focus_widget.get_node().get_value()
                #objectstorageclient.upload_object(path, file)
        elif k == 'tab':
            focus_column = self.listbox.get_focus_column()
            if focus_column == 0:
                self.listbox.set_focus_column(focus_column + 1)
            elif focus_column == 1:
                self.listbox.set_focus_column(focus_column - 1)

def main():
    DirectoryBrowser().main()


#######
# global cache of widgets
_widget_cache = {}

def add_widget(path, widget):
    """Add the widget for a given path"""

    _widget_cache[path] = widget

def get_flagged_names():
    """Return a list of all filenames marked as flagged."""

    l = []
    for w in _widget_cache.values():
        if w.flagged:
            l.append(w.get_node().get_value())
    return l



######
# store path components of initial current working directory
_initial_cwd = []

def store_initial_cwd(name):
    """Store the initial current working directory path components."""

    global _initial_cwd
    _initial_cwd = name.split(dir_sep())

def starts_expanded(name):
    """Return True if directory is a parent of initial cwd."""

    if name is '/':
        return True

    l = name.split(dir_sep())
    if len(l) > len(_initial_cwd):
        return False

    if l != _initial_cwd[:len(l)]:
        return False

    return True


def escape_filename_sh(name):
    """Return a hopefully safe shell-escaped version of a filename."""

    # check whether we have unprintable characters
    for ch in name:
        if ord(ch) < 32:
            # found one so use the ansi-c escaping
            return escape_filename_sh_ansic(name)

    # all printable characters, so return a double-quoted version
    name.replace('\\','\\\\')
    name.replace('"','\\"')
    name.replace('`','\\`')
    name.replace('$','\\$')
    return '"'+name+'"'


def escape_filename_sh_ansic(name):
    """Return an ansi-c shell-escaped version of a filename."""

    out =[]
    # gather the escaped characters into a list
    for ch in name:
        if ord(ch) < 32:
            out.append("\\x%02x"% ord(ch))
        elif ch == '\\':
            out.append('\\\\')
        else:
            out.append(ch)

    # slap them back together in an ansi-c quote  $'...'
    return "$'" + "".join(out) + "'"

SPLIT_RE = re.compile(r'[a-zA-Z]+|\d+')
def alphabetize(s):
    L = []
    for isdigit, group in itertools.groupby(SPLIT_RE.findall(s), key=lambda x: x.isdigit()):
        if isdigit:
            for n in group:
                L.append(('', int(n)))
        else:
            L.append((''.join(group).lower(), 0))
    return L

def dir_sep():
    """Return the separator used in this os."""
    return getattr(os.path,'sep','/')


if __name__=="__main__":
    main()

