import 'dart:async';
import 'dart:convert';

import 'package:flutter/material.dart';
import 'package:shared_preferences/shared_preferences.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

// ======================= مدل پیام =======================

class Msg {
  final int id;
  final String from;
  final String to;
  final String text;
  final int ts;
  int? readAt;

  Msg({
    required this.id,
    required this.from,
    required this.to,
    required this.text,
    required this.ts,
    this.readAt,
  });

  factory Msg.fromJson(Map<String, dynamic> j) => Msg(
        id: j['id'] as int,
        from: j['from'] as String,
        to: j['to'] as String,
        text: j['text'] as String,
        ts: j['ts'] as int,
        readAt: j['readAt'] as int?,
      );
}

// ======================= ارتباط با سرور =======================

class Api extends ChangeNotifier {
  WebSocketChannel? _ch;
  StreamSubscription? _sub;
  Timer? _retry;
  Timer? _ping;
  SharedPreferences? _prefs;

  String serverUrl = '';
  String? username;
  String? token;
  bool connected = false;
  bool authed = false;
  bool _want = false;

  Map<String, dynamic>? _pendingAuth;
  Completer<String?>? _authCompleter;

  final List<String> allUsers = [];
  final Map<String, bool> online = {};
  final List<Msg> messages = [];
  final Map<String, DateTime> _typingAt = {};
  String? openChat;
  DateTime _lastTypingSent = DateTime.fromMillisecondsSinceEpoch(0);

  bool get hasSession => username != null && token != null;

  Future<void> init() async {
    _prefs = await SharedPreferences.getInstance();
    serverUrl = _prefs!.getString('server') ?? '';
    username = _prefs!.getString('user');
    token = _prefs!.getString('token');
    if (hasSession && serverUrl.isNotEmpty) {
      _want = true;
      _connect();
    }
  }

  String _wsUrl(String s) {
    s = s.trim();
    if (s.startsWith('http://')) {
      s = 'ws://${s.substring(7)}';
    } else if (s.startsWith('https://')) {
      s = 'wss://${s.substring(8)}';
    } else if (!s.startsWith('ws://') && !s.startsWith('wss://')) {
      s = 'ws://$s';
    }
    return s;
  }

  Future<String?> authenticate({
    required bool register,
    required String server,
    required String user,
    required String pass,
    String invite = '',
  }) async {
    _closeSync();
    messages.clear();
    allUsers.clear();
    online.clear();
    username = null;
    token = null;
    serverUrl = server.trim();
    _pendingAuth = {
      'type': register ? 'register' : 'login',
      'username': user.trim(),
      'password': pass,
      'invite': invite.trim(),
    };
    final completer = Completer<String?>();
    _authCompleter = completer;
    _want = true;
    _connect();
    return completer.future.timeout(const Duration(seconds: 12), onTimeout: () {
      _pendingAuth = null;
      _authCompleter = null;
      _want = false;
      _closeSync();
      return 'اتصال به سرور برقرار نشد. آدرس رو چک کن.';
    });
  }

  void _connect() {
    _retry?.cancel();
    try {
      final ch = WebSocketChannel.connect(Uri.parse(_wsUrl(serverUrl)));
      _ch = ch;
      _sub = ch.stream.listen(
        _onData,
        onDone: () => _onClosed(ch),
        onError: (_) => _onClosed(ch),
      );
      ch.ready.then((_) {
        if (_ch == ch) _onOpen();
      }).catchError((_) {
        _onClosed(ch);
      });
    } catch (_) {
      _ch = null;
      _failAuth('آدرس سرور درست نیست.');
    }
  }

  void _onOpen() {
    connected = true;
    if (_pendingAuth != null) {
      _send(_pendingAuth!);
    } else if (hasSession) {
      _send({'type': 'resume', 'username': username, 'token': token});
    }
    _ping?.cancel();
    _ping = Timer.periodic(const Duration(seconds: 25), (_) {
      _send({'type': 'ping'});
    });
    notifyListeners();
  }

  void _failAuth(String message) {
    final c = _authCompleter;
    if (c != null && !c.isCompleted) {
      _authCompleter = null;
      _pendingAuth = null;
      _want = false;
      c.complete(message);
    }
  }

  void _onClosed(WebSocketChannel ch) {
    if (_ch != ch) return;
    _ch = null;
    _sub?.cancel();
    _ping?.cancel();
    connected = false;
    authed = false;
    online.clear();
    if (_authCompleter != null) {
      _failAuth('اتصال به سرور برقرار نشد. آدرس رو چک کن.');
      notifyListeners();
      return;
    }
    notifyListeners();
    if (_want) {
      _retry = Timer(const Duration(seconds: 3), _connect);
    }
  }

  void _closeSync() {
    _retry?.cancel();
    _ping?.cancel();
    _sub?.cancel();
    final ch = _ch;
    _ch = null;
    try {
      ch?.sink.close();
    } catch (_) {}
    connected = false;
    authed = false;
  }

  void _send(Map<String, dynamic> m) {
    try {
      _ch?.sink.add(jsonEncode(m));
    } catch (_) {}
  }

  void _onData(dynamic raw) {
    Map<String, dynamic> m;
    try {
      m = jsonDecode(raw as String) as Map<String, dynamic>;
    } catch (_) {
      return;
    }
    switch (m['type']) {
      case 'auth_ok':
        username = m['username'] as String;
        token = m['token'] as String;
        _prefs?.setString('server', serverUrl);
        _prefs?.setString('user', username!);
        _prefs?.setString('token', token!);
        authed = true;
        _pendingAuth = null;
        final c = _authCompleter;
        _authCompleter = null;
        if (c != null && !c.isCompleted) c.complete(null);
        _send({'type': 'users'});
        _send({'type': 'sync', 'afterId': 0});
        notifyListeners();
        break;

      case 'error':
        final code = m['code'];
        final errText = (m['message'] ?? 'خطا') as String;
        if (_authCompleter != null) {
          _failAuth(errText);
          _closeSync();
        } else if (code == 'bad_token') {
          logout();
        }
        notifyListeners();
        break;

      case 'users':
        final usersList = (m['users'] as List).cast<Map<String, dynamic>>();
        for (final u in usersList) {
          final name = u['username'] as String;
          if (!allUsers.contains(name)) allUsers.add(name);
          online[name] = u['online'] == true;
        }
        notifyListeners();
        break;

      case 'presence':
        final pname = m['username'] as String;
        if (pname != username && !allUsers.contains(pname)) allUsers.add(pname);
        online[pname] = m['online'] == true;
        notifyListeners();
        break;

      case 'sync':
        final syncList = (m['messages'] as List).cast<Map<String, dynamic>>();
        for (final j in syncList) {
          _addMsg(Msg.fromJson(j));
        }
        final open = openChat;
        if (open != null) markRead(open);
        notifyListeners();
        break;

      case 'message':
        final newMsg = Msg.fromJson(m['message'] as Map<String, dynamic>);
        _addMsg(newMsg);
        if (newMsg.from == openChat) markRead(newMsg.from);
        notifyListeners();
        break;

      case 'typing':
        final from = m['from'] as String;
        _typingAt[from] = DateTime.now();
        Timer(const Duration(milliseconds: 3200), notifyListeners);
        notifyListeners();
        break;

      case 'read':
        final by = m['by'] as String;
        final upTo = m['upToId'] as int;
        final now = DateTime.now().millisecondsSinceEpoch;
        for (final x in messages) {
          if (x.from == username && x.to == by && x.id <= upTo && x.readAt == null) {
            x.readAt = now;
          }
        }
        notifyListeners();
        break;
    }
  }

  void _addMsg(Msg msg) {
    if (messages.any((x) => x.id == msg.id)) return;
    if (msg.from != username && msg.to != username) return;
    final other = msg.from == username ? msg.to : msg.from;
    if (!allUsers.contains(other)) allUsers.add(other);
    messages.add(msg);
    messages.sort((a, b) => a.id.compareTo(b.id));
  }

  // ---------- کارهایی که صفحه‌ها صدا می‌زنن ----------

  List<Msg> chatWith(String peer) => messages
      .where((x) =>
          (x.from == peer && x.to == username) ||
          (x.from == username && x.to == peer))
      .toList();

  int unreadFrom(String peer) => messages
      .where((x) => x.from == peer && x.to == username && x.readAt == null)
      .length;

  bool isTyping(String peer) {
    final t = _typingAt[peer];
    return t != null && DateTime.now().difference(t).inMilliseconds < 3000;
  }

  bool sendText(String to, String text) {
    final t = text.trim();
    if (t.isEmpty || !authed) return false;
    _send({'type': 'send', 'to': to, 'text': t});
    return true;
  }

  void notifyTyping(String to) {
    final now = DateTime.now();
    if (now.difference(_lastTypingSent).inMilliseconds < 2000) return;
    _lastTypingSent = now;
    _send({'type': 'typing', 'to': to});
  }

  void markRead(String peer) {
    int maxId = 0;
    final now = DateTime.now().millisecondsSinceEpoch;
    for (final x in messages) {
      if (x.from == peer && x.to == username && x.readAt == null) {
        if (x.id > maxId) maxId = x.id;
        x.readAt = now;
      }
    }
    if (maxId > 0) {
      _send({'type': 'read', 'with': peer, 'upToId': maxId});
    }
  }

  void logout() {
    _want = false;
    _closeSync();
    _prefs?.remove('user');
    _prefs?.remove('token');
    username = null;
    token = null;
    messages.clear();
    allUsers.clear();
    online.clear();
    notifyListeners();
  }
}

// ======================= برنامه =======================

const kBg = Color(0xFF0E1621);
const kSurface = Color(0xFF17212B);
const kMine = Color(0xFF2B5278);
const kTheirs = Color(0xFF182533);
const kAccent = Color(0xFF5EB5F7);

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();
  final api = Api();
  await api.init();
  runApp(App(api));
}

class App extends StatelessWidget {
  final Api api;
  const App(this.api, {super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      debugShowCheckedModeBanner: false,
      title: 'Messenger',
      theme: ThemeData(
        brightness: Brightness.dark,
        scaffoldBackgroundColor: kBg,
        colorScheme: ColorScheme.fromSeed(
          seedColor: kAccent,
          brightness: Brightness.dark,
          surface: kSurface,
        ),
        appBarTheme: const AppBarTheme(
          backgroundColor: kSurface,
          foregroundColor: Colors.white,
          elevation: 0,
        ),
        useMaterial3: true,
      ),
      builder: (context, child) =>
          Directionality(textDirection: TextDirection.rtl, child: child!),
      home: AnimatedBuilder(
        animation: api,
        builder: (context, _) =>
            api.hasSession ? ChatListScreen(api) : LoginScreen(api),
      ),
    );
  }
}

// ======================= صفحه ورود =======================

class LoginScreen extends StatefulWidget {
  final Api api;
  const LoginScreen(this.api, {super.key});

  @override
  State<LoginScreen> createState() => _LoginScreenState();
}

class _LoginScreenState extends State<LoginScreen> {
  late final TextEditingController server;
  final user = TextEditingController();
  final pass = TextEditingController();
  final invite = TextEditingController();
  bool register = false;
  bool loading = false;
  String? error;

  @override
  void initState() {
    super.initState();
    server = TextEditingController(
        text: widget.api.serverUrl.isEmpty ? '127.0.0.1:8080' : widget.api.serverUrl);
  }

  Future<void> submit() async {
    if (server.text.trim().isEmpty || user.text.trim().isEmpty || pass.text.isEmpty) {
      setState(() => error = 'همه‌ی فیلدها رو پر کن.');
      return;
    }
    setState(() {
      loading = true;
      error = null;
    });
    final err = await widget.api.authenticate(
      register: register,
      server: server.text,
      user: user.text,
      pass: pass.text,
      invite: invite.text,
    );
    if (!mounted) return;
    setState(() {
      loading = false;
      error = err;
    });
  }

  Widget field(TextEditingController c, String label,
      {bool obscure = false, bool ltr = true}) {
    return Padding(
      padding: const EdgeInsets.only(bottom: 12),
      child: TextField(
        controller: c,
        obscureText: obscure,
        textDirection: ltr ? TextDirection.ltr : null,
        textAlign: ltr ? TextAlign.left : TextAlign.start,
        autocorrect: false,
        decoration: InputDecoration(
          labelText: label,
          filled: true,
          fillColor: kSurface,
          border: OutlineInputBorder(
            borderRadius: BorderRadius.circular(12),
            borderSide: BorderSide.none,
          ),
        ),
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      body: SafeArea(
        child: Center(
          child: SingleChildScrollView(
            padding: const EdgeInsets.all(24),
            child: Column(
              mainAxisSize: MainAxisSize.min,
              crossAxisAlignment: CrossAxisAlignment.stretch,
              children: [
                const Icon(Icons.send_rounded, size: 72, color: kAccent),
                const SizedBox(height: 8),
                Text(
                  register ? 'ساخت حساب' : 'ورود',
                  textAlign: TextAlign.center,
                  style: const TextStyle(fontSize: 24, fontWeight: FontWeight.bold),
                ),
                const SizedBox(height: 24),
                field(server, 'آدرس سرور (مثلا 127.0.0.1:8080)'),
                field(user, 'نام کاربری (انگلیسی)'),
                field(pass, 'رمز عبور', obscure: true),
                if (register) field(invite, 'کد دعوت (اگه سرور خواسته)'),
                if (error != null)
                  Padding(
                    padding: const EdgeInsets.only(bottom: 12),
                    child: Text(error!,
                        textAlign: TextAlign.center,
                        style: const TextStyle(color: Colors.redAccent)),
                  ),
                FilledButton(
                  onPressed: loading ? null : submit,
                  style: FilledButton.styleFrom(
                      padding: const EdgeInsets.symmetric(vertical: 14)),
                  child: loading
                      ? const SizedBox(
                          height: 20,
                          width: 20,
                          child: CircularProgressIndicator(strokeWidth: 2))
                      : Text(register ? 'ثبت‌نام' : 'ورود'),
                ),
                TextButton(
                  onPressed: loading
                      ? null
                      : () => setState(() {
                            register = !register;
                            error = null;
                          }),
                  child: Text(register ? 'حساب دارم، ورود' : 'حساب ندارم، ثبت‌نام'),
                ),
              ],
            ),
          ),
        ),
      ),
    );
  }
}

// ======================= لیست چت‌ها =======================

String hhmm(int ts) {
  final d = DateTime.fromMillisecondsSinceEpoch(ts);
  final h = d.hour.toString().padLeft(2, '0');
  final m = d.minute.toString().padLeft(2, '0');
  return '$h:$m';
}

class ChatListScreen extends StatelessWidget {
  final Api api;
  const ChatListScreen(this.api, {super.key});

  @override
  Widget build(BuildContext context) {
    final peers = [...api.allUsers];
    int lastTs(String p) {
      final c = api.chatWith(p);
      return c.isEmpty ? 0 : c.last.ts;
    }

    peers.sort((a, b) => lastTs(b).compareTo(lastTs(a)));

    return Scaffold(
      appBar: AppBar(
        title: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            const Text('پیام‌رسان'),
            Text(
              api.authed ? 'متصل' : 'در حال اتصال...',
              style: const TextStyle(fontSize: 12, color: Colors.white60),
            ),
          ],
        ),
        actions: [
          IconButton(
            icon: const Icon(Icons.logout),
            tooltip: 'خروج',
            onPressed: () async {
              final ok = await showDialog<bool>(
                context: context,
                builder: (ctx) => AlertDialog(
                  title: const Text('خروج از حساب؟'),
                  actions: [
                    TextButton(
                        onPressed: () => Navigator.pop(ctx, false),
                        child: const Text('نه')),
                    TextButton(
                        onPressed: () => Navigator.pop(ctx, true),
                        child: const Text('خروج')),
                  ],
                ),
              );
              if (ok == true) api.logout();
            },
          ),
        ],
      ),
      body: peers.isEmpty
          ? const Center(
              child: Padding(
                padding: EdgeInsets.all(24),
                child: Text(
                  'هنوز کاربر دیگه‌ای ثبت‌نام نکرده.\nبقیه رو دعوت کن!',
                  textAlign: TextAlign.center,
                  style: TextStyle(color: Colors.white60),
                ),
              ),
            )
          : ListView.separated(
              itemCount: peers.length,
              separatorBuilder: (_, __) =>
                  const Divider(height: 1, color: Color(0xFF0B121A)),
              itemBuilder: (context, i) {
                final p = peers[i];
                final chat = api.chatWith(p);
                final unread = api.unreadFrom(p);
                final isOn = api.online[p] == true;
                return ListTile(
                  leading: Stack(
                    children: [
                      CircleAvatar(
                        radius: 24,
                        backgroundColor: kMine,
                        child: Text(p[0].toUpperCase(),
                            style: const TextStyle(
                                fontSize: 20, fontWeight: FontWeight.bold)),
                      ),
                      if (isOn)
                        Positioned(
                          right: 0,
                          bottom: 0,
                          child: Container(
                            width: 13,
                            height: 13,
                            decoration: BoxDecoration(
                              color: Colors.greenAccent,
                              shape: BoxShape.circle,
                              border: Border.all(color: kBg, width: 2),
                            ),
                          ),
                        ),
                    ],
                  ),
                  title: Text(p, textDirection: TextDirection.ltr, textAlign: TextAlign.start),
                  subtitle: Text(
                    api.isTyping(p)
                        ? 'در حال نوشتن...'
                        : (chat.isEmpty ? 'شروع گفتگو' : chat.last.text),
                    maxLines: 1,
                    overflow: TextOverflow.ellipsis,
                    style: TextStyle(
                        color: api.isTyping(p) ? kAccent : Colors.white60),
                  ),
                  trailing: Column(
                    mainAxisAlignment: MainAxisAlignment.center,
                    crossAxisAlignment: CrossAxisAlignment.end,
                    children: [
                      if (chat.isNotEmpty)
                        Text(hhmm(chat.last.ts),
                            style: const TextStyle(
                                fontSize: 12, color: Colors.white54)),
                      if (unread > 0) ...[
                        const SizedBox(height: 4),
                        Container(
                          padding: const EdgeInsets.symmetric(
                              horizontal: 7, vertical: 2),
                          decoration: BoxDecoration(
                            color: kAccent,
                            borderRadius: BorderRadius.circular(12),
                          ),
                          child: Text('$unread',
                              style: const TextStyle(
                                  color: Colors.black,
                                  fontSize: 12,
                                  fontWeight: FontWeight.bold)),
                        ),
                      ],
                    ],
                  ),
                  onTap: () => Navigator.push(
                    context,
                    MaterialPageRoute(builder: (_) => ChatScreen(api, p)),
                  ),
                );
              },
            ),
    );
  }
}

// ======================= صفحه چت =======================

class ChatScreen extends StatefulWidget {
  final Api api;
  final String peer;
  const ChatScreen(this.api, this.peer, {super.key});

  @override
  State<ChatScreen> createState() => _ChatScreenState();
}

class _ChatScreenState extends State<ChatScreen> {
  final controller = TextEditingController();

  @override
  void initState() {
    super.initState();
    widget.api.openChat = widget.peer;
    widget.api.markRead(widget.peer);
  }

  @override
  void dispose() {
    widget.api.openChat = null;
    controller.dispose();
    super.dispose();
  }

  void send() {
    final ok = widget.api.sendText(widget.peer, controller.text);
    if (ok) {
      controller.clear();
    } else if (!widget.api.authed) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('به سرور وصل نیستی. چند لحظه صبر کن.')),
      );
    }
  }

  @override
  Widget build(BuildContext context) {
    final api = widget.api;
    return AnimatedBuilder(
      animation: api,
      builder: (context, _) {
        final msgs = api.chatWith(widget.peer).reversed.toList();
        final typing = api.isTyping(widget.peer);
        final isOn = api.online[widget.peer] == true;
        return Scaffold(
          appBar: AppBar(
            title: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text(widget.peer, textDirection: TextDirection.ltr),
                Text(
                  typing ? 'در حال نوشتن...' : (isOn ? 'آنلاین' : 'آفلاین'),
                  style: TextStyle(
                      fontSize: 12, color: typing || isOn ? kAccent : Colors.white60),
                ),
              ],
            ),
          ),
          body: Column(
            children: [
              Expanded(
                child: msgs.isEmpty
                    ? const Center(
                        child: Text('هنوز پیامی نیست. سلام کن!',
                            style: TextStyle(color: Colors.white54)))
                    : ListView.builder(
                        reverse: true,
                        padding: const EdgeInsets.symmetric(
                            horizontal: 10, vertical: 8),
                        itemCount: msgs.length,
                        itemBuilder: (context, i) =>
                            Bubble(msgs[i], msgs[i].from == api.username),
                      ),
              ),
              SafeArea(
                top: false,
                child: Container(
                  color: kSurface,
                  padding: const EdgeInsets.fromLTRB(8, 6, 8, 6),
                  child: Row(
                    children: [
                      Expanded(
                        child: TextField(
                          controller: controller,
                          minLines: 1,
                          maxLines: 5,
                          onChanged: (_) => api.notifyTyping(widget.peer),
                          decoration: InputDecoration(
                            hintText: 'پیام...',
                            filled: true,
                            fillColor: kBg,
                            contentPadding: const EdgeInsets.symmetric(
                                horizontal: 16, vertical: 10),
                            border: OutlineInputBorder(
                              borderRadius: BorderRadius.circular(24),
                              borderSide: BorderSide.none,
                            ),
                          ),
                        ),
                      ),
                      const SizedBox(width: 6),
                      IconButton.filled(
                        onPressed: send,
                        icon: const Icon(Icons.send_rounded),
                      ),
                    ],
                  ),
                ),
              ),
            ],
          ),
        );
      },
    );
  }
}

class Bubble extends StatelessWidget {
  final Msg msg;
  final bool mine;
  const Bubble(this.msg, this.mine, {super.key});

  @override
  Widget build(BuildContext context) {
    final maxW = MediaQuery.of(context).size.width * 0.78;
    return Align(
      alignment: mine ? Alignment.centerRight : Alignment.centerLeft,
      child: Container(
        constraints: BoxConstraints(maxWidth: maxW),
        margin: const EdgeInsets.symmetric(vertical: 2),
        padding: const EdgeInsets.fromLTRB(12, 7, 12, 5),
        decoration: BoxDecoration(
          color: mine ? kMine : kTheirs,
          borderRadius: BorderRadius.only(
            topLeft: const Radius.circular(16),
            topRight: const Radius.circular(16),
            bottomLeft: Radius.circular(mine ? 16 : 4),
            bottomRight: Radius.circular(mine ? 4 : 16),
          ),
        ),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.end,
          mainAxisSize: MainAxisSize.min,
          children: [
            Align(
              alignment: Alignment.centerRight,
              child: Text(msg.text, style: const TextStyle(fontSize: 16)),
            ),
            const SizedBox(height: 2),
            Row(
              mainAxisSize: MainAxisSize.min,
              children: [
                Text(hhmm(msg.ts),
                    style: const TextStyle(fontSize: 11, color: Colors.white54)),
                if (mine) ...[
                  const SizedBox(width: 4),
                  Icon(
                    msg.readAt != null ? Icons.done_all : Icons.done,
                    size: 15,
                    color: msg.readAt != null ? kAccent : Colors.white54,
                  ),
                ],
              ],
            ),
          ],
        ),
      ),
    );
  }
}
