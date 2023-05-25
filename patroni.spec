# -*- mode: python -*-

block_cipher = None


def hiddenimports():
    import sys
    sys.path.insert(0, '.')
    try:
        import patroni.dcs
        return patroni.dcs.dcs_modules() + ['http.server']
    finally:
        sys.path.pop(0)


a = Analysis(['patroni/__main__.py'],
             pathex=[],
             binaries=None,
             datas=[
                ('patroni/postgresql/available_parameters/*.yml', 'patroni/postgresql/available_parameters'),
                ('patroni/postgresql/available_parameters/*.yaml', 'patroni/postgresql/available_parameters'),
            ],
             hiddenimports=hiddenimports(),
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
