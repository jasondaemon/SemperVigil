#!/usr/bin/env python3
"""Build a complete tracked source overlay, never a baseline-relative partial copy."""
import argparse,hashlib,json,pathlib,shutil,subprocess,tempfile

def build(repo,output,bases):
    repo=repo.resolve();output.mkdir(parents=True,exist_ok=True)
    dirty=subprocess.check_output(['git','-C',str(repo),'status','--porcelain','--','src']).decode()
    if dirty:raise ValueError('runtime_source_must_be_committed')
    commit=subprocess.check_output(['git','-C',str(repo),'rev-parse','HEAD'],text=True).strip()
    names=subprocess.check_output(['git','-C',str(repo),'ls-files','-z','src/sempervigil']).decode().split('\0')
    context=pathlib.Path(tempfile.mkdtemp(prefix='sv-runtime-',dir=output));hashes={}
    for name in filter(None,names):
        source=repo/name
        if source.is_symlink() or not source.is_file():raise ValueError('runtime_source_must_be_regular_file')
        target=context/'package'/pathlib.Path(name).relative_to('src/sempervigil');target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(source,target);target.chmod(0o644)
        hashes[str(target.relative_to(context/'package'))]=hashlib.sha256(target.read_bytes()).hexdigest()
    for name,expected in [('event_report_contract.py','d25229d8a72aa7b058e34705fdb744dd39c409312c158e6746085301ca9399fb'),('event_source_report_pilot.py','aa03612770c6f3fef62e6af39d185d818246d88ed3c457d04d72589a043948ba')]:
        assert hashes[name]==expected,'protected legacy source changed'
    (context/'Dockerfile').write_text('ARG BASE\nFROM ${BASE}\nCOPY package /app/src/sempervigil\nLABEL org.opencontainers.image.revision="'+commit+'" sempervigil.source.packaging="complete-tracked-source-v1"\n')
    images={bases['ingest']:'sempervigil-ingest:ed367b2-v2'+commit[:7],bases['builder']:'sempervigil-builder:'+commit[:7]+'-hugo6d1f91ff'}
    for base,image in images.items():
        print('Building',image,'from',base,flush=True)
        with (output/(image.replace(':','-')+'-build.log')).open('w') as log:
            subprocess.run(['docker','buildx','build','--network','none','--platform','linux/amd64','--load','--build-arg','BASE='+base,'-t',image,str(context)],stdout=log,stderr=subprocess.STDOUT,check=True)
    manifest={'workflow':'complete-tracked-runtime-source-v1','commit':commit,'images':images,'source_files':hashes,'context':str(context)}
    (output/'runtime-image-manifest.json').write_text(json.dumps(manifest,indent=2));return manifest

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--output',required=True,type=pathlib.Path);p.add_argument('--ingest-base',required=True);p.add_argument('--builder-base',required=True);a=p.parse_args()
    build(pathlib.Path(__file__).resolve().parents[1],a.output,{'ingest':a.ingest_base,'builder':a.builder_base})
