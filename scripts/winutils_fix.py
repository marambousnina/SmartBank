"""
Fix Windows + PySpark : configure HADOOP_HOME automatiquement
sans avoir besoin d'installer Hadoop manuellement.
"""
import os
import sys
import tempfile
import struct

def setup_hadoop_windows():
    """
    Crée un winutils.exe factice dans un dossier temporaire
    et configure HADOOP_HOME pour que PySpark puisse démarrer.
    """
    # Créer le dossier hadoop/bin temporaire
    hadoop_home = os.path.join(tempfile.gettempdir(), "hadoop_winutils")
    hadoop_bin  = os.path.join(hadoop_home, "bin")
    os.makedirs(hadoop_bin, exist_ok=True)

    winutils_path = os.path.join(hadoop_bin, "winutils.exe")

    # Créer un winutils.exe minimal (exécutable Windows vide mais valide)
    if not os.path.exists(winutils_path):
        # PE header minimal pour un .exe Windows valide
        pe_stub = (
            b'MZ'                          # DOS signature
            + b'\x90\x00' * 29            # DOS stub padding
            + b'\x3c\x00\x00\x00'         # PE offset = 0x3c
            + b'PE\x00\x00'               # PE signature
            + b'\x64\x86'                 # Machine: x86-64
            + b'\x00\x00'                 # NumberOfSections
            + b'\x00' * 12               # timestamps etc
            + b'\xf0\x00'                 # SizeOfOptionalHeader
            + b'\x22\x00'                 # Characteristics
            + b'\x0b\x02'                 # Magic: PE32+
            + b'\x00' * 230              # rest of optional header
        )
        with open(winutils_path, 'wb') as f:
            f.write(pe_stub)

    # Configurer les variables d'environnement
    os.environ["HADOOP_HOME"]       = hadoop_home
    os.environ["hadoop.home.dir"]   = hadoop_home
    os.environ["PATH"]              = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

    return hadoop_home


if __name__ == "__main__":
    path = setup_hadoop_windows()
    print(f"✅ HADOOP_HOME configuré : {path}")