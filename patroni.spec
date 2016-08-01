# -*- mode: python -*-

block_cipher = None


a = Analysis(['patroni/__main__.py', 'patroni/dcs/consul.py', 'patroni/dcs/etcd.py', 'patroni/dcs/exhibitor.py', 'patroni/dcs/zookeeper.py'],
             pathex=[],
             binaries=None,
             datas=None,
             hiddenimports=['patroni.dcs.consul', 'patroni.dcs.etcd', 'patroni.dcs.exhibitor', 'patroni.dcs.zookeeper'],
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    name='patroni',
    debug=False,
    strip=False,
    upx=True,
    console=True)
