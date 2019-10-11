import tarfile
import os


def copy_to(src, dst, container):
    """Copies file from host to postgres container
    Arguments:
    :src: filepath in host filesystem
    :dst: filepath in the container's filesystem
    :container: a docker py container object"""
    tar = tarfile.open(src + '.tar', mode='w')
    try:
        tar.add(src, arcname=os.path.basename(src))
    finally:
        tar.close()
    data = open(src + '.tar', 'rb').read()
    container.put_archive(os.path.dirname(dst), data)
    os.remove(src + '.tar')


def get_from(src, dst, container):
    """Copies file from postgres container to host
    Arguments:
    :src: filepath in container's filesystem
    :dst: filepath in the host's filesystem
    :container: a docker py container object"""
    bits, stats = container.get_archive(src)
    tmp_tar_path = os.path.join(dst, 'tmp.tar')
    f = open(tmp_tar_path, 'wb')
    for chunk in bits:
        f.write(chunk)
    f.close
    tar = tarfile.open(tmp_tar_path)
    tar.extractall(dst)
    os.remove(tmp_tar_path)
