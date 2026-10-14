/* Accounts Hub — Supabase backend.
   Exposes the same small document-store surface the hub page was written against
   (collection().where().limit().onSnapshot(), doc().get/set/update/delete, add) plus a `user` helper,
   backed by the `records` and `members` tables in schema.sql. Requires window.supabase (supabase-js v2) and config.js. */
(function () {
  'use strict';
  var cfg = window.HUB_CONFIG || {};
  var sb = null;
  function client() {
    if (!sb) sb = window.supabase.createClient(cfg.supabaseUrl, cfg.supabaseAnonKey, { auth: { persistSession: true, detectSessionInUrl: true } });
    return sb;
  }
  function err(code, message) { return { code: code, message: message }; }
  function mapErr(e) {
    if (!e) return err('unavailable', 'Unknown error');
    var m = String(e.message || e);
    if (/row-level security|permission|42501/i.test(m)) return err('invalid_argument', 'Not allowed: ' + m);
    return err('unavailable', m);
  }
  var COLS = { account: 'account', status: 'status', __id: 'id' };

  function snapDoc(row) {
    var data = row ? Object.freeze(Object.assign({}, row.data)) : undefined;
    return { id: row ? row.id : '', exists: !!row, data: function () { return data; }, metadata: { fromCache: false, hasPendingWrites: false } };
  }

  function Query(collection, filters, max) {
    this.collection = collection; this.filters = filters || []; this.max = max || 1000;
  }
  Query.prototype.where = function (field, op, value) {
    return new Query(this.collection, this.filters.concat([[field, op, value]]), this.max);
  };
  Query.prototype.limit = function (n) { return new Query(this.collection, this.filters, n); };
  Query.prototype._fetch = async function () {
    var rows = [], from = 0, page = 1000;
    while (rows.length < this.max) {
      var q = client().from('records').select('id,data').eq('collection', this.collection).order('id').range(from, from + page - 1);
      this.filters.forEach(function (f) {
        var col = COLS[f[0]] || ('data->>' + f[0]);
        if (f[1] === '==') q = q.eq(col, f[2]);
        else if (f[1] === '!=') q = q.neq(col, f[2]);
        else if (f[1] === 'in') q = q.in(col, f[2]);
      });
      var res = await q;
      if (res.error) throw mapErr(res.error);
      rows = rows.concat(res.data);
      if (res.data.length < page) break;
      from += page;
    }
    return rows.slice(0, this.max);
  };
  Query.prototype.get = async function () {
    var rows = await this._fetch(), docs = rows.map(snapDoc);
    return { docs: docs, size: docs.length, empty: !docs.length, docChanges: function () { return []; }, metadata: { fromCache: false, hasPendingWrites: false } };
  };
  Query.prototype.onSnapshot = function (next, onError) {
    var self = this, alive = true, timer = null, chan = null;
    function load() {
      self.get().then(function (s) { if (alive) next(s); }, function (e) { if (alive && onError) onError(e); });
    }
    load();
    chan = client().channel('rec-' + self.collection + '-' + Math.random().toString(36).slice(2))
      .on('postgres_changes', { event: '*', schema: 'public', table: 'records', filter: 'collection=eq.' + self.collection }, function () {
        clearTimeout(timer); timer = setTimeout(load, 250);
      })
      .subscribe();
    var poll = setInterval(load, 60000);  // safety net if realtime drops
    return function () { alive = false; clearTimeout(timer); clearInterval(poll); if (chan) client().removeChannel(chan); };
  };

  function Doc(collection, id) { this.collection = collection; this.id = id; this.path = collection + '/' + id; }
  Doc.prototype.get = async function () {
    var res = await client().from('records').select('id,data').eq('collection', this.collection).eq('id', this.id).maybeSingle();
    if (res.error) throw mapErr(res.error);
    return snapDoc(res.data);
  };
  Doc.prototype.set = async function (data) {
    var res = await client().from('records').upsert({ collection: this.collection, id: this.id, data: data });
    if (res.error) throw mapErr(res.error);
  };
  Doc.prototype.update = async function (patch) {
    var cur = await this.get();
    if (!cur.exists) throw err('invalid_argument', 'Document does not exist');
    var res = await client().from('records').update({ data: Object.assign({}, cur.data(), patch) }).eq('collection', this.collection).eq('id', this.id);
    if (res.error) throw mapErr(res.error);
  };
  Doc.prototype.delete = async function () {
    var res = await client().from('records').delete().eq('collection', this.collection).eq('id', this.id);
    if (res.error) throw mapErr(res.error);
  };
  Doc.prototype.onSnapshot = function (next, onError) {
    var self = this;
    return new Query(this.collection, [['__id', '==', this.id]]).onSnapshot(function (s) { next(s.docs[0] || snapDoc(null)); }, onError);
  };

  function Collection(name) { Query.call(this, name, [], 1000); this.path = name; }
  Collection.prototype = Object.create(Query.prototype);
  Collection.prototype.doc = function (id) {
    return new Doc(this.path, id || (Date.now().toString(36) + Math.random().toString(36).slice(2, 10)));
  };
  Collection.prototype.add = async function (data) { var d = this.doc(); await d.set(data); return d; };

  var db = {
    collection: function (name) { return new Collection(name); },
    doc: function (path) { var p = path.split('/'); return new Doc(p[0], p.slice(1).join('/')); }
  };

  var me = null, memberRows = null;
  async function members() {
    if (!memberRows) {
      var res = await client().from('members').select('email,name,user_id,role');
      memberRows = res.error ? [] : res.data;
    }
    return memberRows;
  }
  var user = {
    id: async function () { return me ? me.id : null; },
    isOwner: async function () { return false; },
    canEdit: async function () { var m = (await members()).find(function (x) { return me && x.email.toLowerCase() === (me.email || '').toLowerCase(); }); return !!m && m.role === 'editor'; },
    can: async function (what) { return what === 'data.write' ? user.canEdit() : null; },
    profiles: async function (ids) {
      var list = await members(), out = {};
      (Array.isArray(ids) ? ids : [ids]).forEach(function (id) {
        var m = list.find(function (x) { return x.user_id === id; });
        var name = m ? (m.name || m.email) : '';
        out[id] = { id: id, name: name, isMe: !!me && me.id === id, email: m ? m.email : null, guest: false, color: '#24415e',
          avatarUrl: 'data:image/svg+xml;utf8,' + encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" width="40" height="40"><rect width="40" height="40" rx="20" fill="#24415e"/><text x="20" y="26" font-family="sans-serif" font-size="16" fill="#fff" text-anchor="middle">' + (name || '?').charAt(0).toUpperCase() + '</text></svg>') };
      });
      return out;
    }
  };

  window.HubBackend = {
    configured: function () { return !!(cfg.supabaseUrl && cfg.supabaseAnonKey && !/YOUR-/.test(cfg.supabaseUrl)); },
    session: async function () {
      var s = await client().auth.getSession();
      me = s.data.session ? s.data.session.user : null;
      if (me) { try { await client().rpc('claim_member'); } catch (e) {} }
      return me;
    },
    signIn: async function (email) {
      var res = await client().auth.signInWithOtp({ email: email, options: { emailRedirectTo: location.origin + location.pathname, shouldCreateUser: true } });
      if (res.error) throw res.error;
    },
    signOut: async function () { await client().auth.signOut(); location.reload(); },
    isMember: async function () { var m = await members(); return m.length > 0; },
    email: function () { return me ? me.email : ''; },
    db: db,
    user: user
  };
})();
