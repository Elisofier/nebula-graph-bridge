from app import nebula_graph

space = 'test'
port = '30301'
ip = '10.26.24.59'
nebula = nebula_graph.Nebula(ip, port, 'root', 'nebula', space)

_account = {
    'id': "1",
}
account = {}
for acp in _account:
    if _account[acp] is not None:
        account[acp] = _account[acp]

vid = "account_twitter?id=1"
entity = {
    'info': account,
    'tag_name': 'account',
    'vid': vid
}
nebula.update_entity(entity)
# nebula.insert_entity(entity)
