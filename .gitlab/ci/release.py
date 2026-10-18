#!/usr/bin/env python3
"""Publish the dev alias, prune GitLab tags, and roll out configured workloads."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import ssl
import subprocess
import urllib.error
import urllib.parse
import urllib.request


TAG = re.compile(r"^dev-([0-9]+)-[0-9a-f]{8,40}$")
ACCEPT = ", ".join([
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.docker.distribution.manifest.v2+json",
])


def tls_context():
    context = ssl.create_default_context()
    for variable in ("CI_SERVER_TLS_CA_FILE", "REGISTRY_CA_FILE"):
        if os.environ.get(variable):
            context.load_verify_locations(os.environ[variable])
    return context


def request(url, method="GET", headers=None, data=None):
    req = urllib.request.Request(url, method=method, headers=headers or {}, data=data)
    return urllib.request.urlopen(req, context=tls_context(), timeout=60)


def api(path, method="GET"):
    token = os.environ["DEV_REGISTRY_API_TOKEN"]
    with request(f"{os.environ['CI_API_V4_URL']}/{path}", method, {"PRIVATE-TOKEN": token}) as response:
        body = response.read()
        return (json.loads(body) if body else None), response.headers


def pages(path):
    page = "1"
    result = []
    while page:
        separator = "&" if "?" in path else "?"
        values, headers = api(f"{path}{separator}per_page=100&page={page}")
        result.extend(values)
        page = headers.get("X-Next-Page", "")
    return result


def current():
    project = os.environ["CI_PROJECT_ID"]
    branch, _ = api(f"projects/{project}/repository/branches/dev")
    pipelines, _ = api(f"projects/{project}/pipelines?ref=dev&order_by=id&sort=desc&per_page=1")
    return (branch["commit"]["id"] == os.environ["CI_COMMIT_SHA"]
            and bool(pipelines) and str(pipelines[0]["id"]) == os.environ["CI_PIPELINE_ID"])


def image_names():
    result = []
    for line in Path(".gitlab/ci/images.txt").read_text().splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        name, _ = line.split()
        result.append(os.environ["CI_REGISTRY_IMAGE"] + ("" if name == "app" else f"/{name}"))
    return result


def image_tag():
    return f"dev-{os.environ['CI_PIPELINE_ID']}-{os.environ['CI_COMMIT_SHORT_SHA']}"


def registry_auth(image):
    registry, repository = image.split("/", 1)
    url = f"https://{registry}/v2/{repository}/manifests/{image_tag()}"
    try:
        with request(url, headers={"Accept": ACCEPT}) as response:
            return {}, response.read(), response.headers["Content-Type"]
    except urllib.error.HTTPError as error:
        if error.code != 401:
            raise
        challenge = error.headers.get("WWW-Authenticate", "")
    if not challenge.lower().startswith("bearer "):
        raise RuntimeError("Registry did not offer bearer authentication")
    fields = dict(re.findall(r'(\w+)="([^"]*)"', challenge))
    realm = urllib.parse.urlsplit(fields["realm"])
    if realm.scheme != "https":
        raise RuntimeError("Registry token endpoint must use HTTPS")
    allowed_hosts = {registry.split(":")[0], urllib.parse.urlsplit(os.environ["CI_API_V4_URL"]).hostname}
    if realm.hostname not in allowed_hosts:
        raise RuntimeError("Unexpected registry authentication host")
    query = urllib.parse.urlencode({"service": fields.get("service", "container_registry"),
                                   "scope": f"repository:{repository}:pull,push"})
    credentials = base64.b64encode(
        f"{os.environ['CI_REGISTRY_USER']}:{os.environ['CI_REGISTRY_PASSWORD']}".encode()
    ).decode()
    token_url = fields["realm"] + ("&" if "?" in fields["realm"] else "?") + query
    with request(token_url, headers={"Authorization": f"Basic {credentials}"}) as response:
        token = json.load(response)
    auth = {"Authorization": f"Bearer {token.get('token') or token['access_token']}"}
    with request(url, headers={**auth, "Accept": ACCEPT}) as response:
        return auth, response.read(), response.headers["Content-Type"]


def obsolete(tags, keep=8, preserve=()):
    builds = [tag["name"] for tag in tags if TAG.fullmatch(tag["name"])]
    builds.sort(key=lambda value: (int(TAG.fullmatch(value).group(1)), value), reverse=True)
    retained = set(preserve) & set(builds)
    for tag in builds:
        if len(retained) >= keep:
            break
        retained.add(tag)
    return [tag for tag in builds if tag not in retained]


def prune():
    project = os.environ["CI_PROJECT_ID"]
    expected = {image.split("/", 1)[1] for image in image_names()}
    repositories = pages(f"projects/{project}/registry/repositories")
    for repository in repositories:
        if repository["path"] not in expected:
            continue
        path = f"projects/{project}/registry/repositories/{repository['id']}/tags"
        tags = pages(path)
        # Preserve the image referenced by dev, even if newer builds failed.
        preserve = []
        if any(tag["name"] == "dev" for tag in tags):
            alias, _ = api(f"{path}/dev")
            for tag in tags:
                if TAG.fullmatch(tag["name"]):
                    details, _ = api(f"{path}/{tag['name']}")
                    if details.get("digest") == alias.get("digest"):
                        preserve.append(tag["name"])
            preserve = sorted(preserve, key=lambda tag: int(TAG.fullmatch(tag).group(1)), reverse=True)[:1]
        for tag in obsolete(tags, preserve=preserve):
            api(f"{path}/{urllib.parse.quote(tag, safe='')}", "DELETE")
            print(f"Removed {repository['path']}:{tag}", flush=True)
        if obsolete(pages(path), preserve=preserve):
            raise RuntimeError(f"Retention verification failed for {repository['path']}")


def publish():
    Path("dev-release.env").write_text("DEV_PUBLISHED=false\n")
    if not current():
        print("A newer dev pipeline exists; skipping alias publication.")
        return
    manifests = [(image, *registry_auth(image)) for image in image_names()]
    if not current():
        print("Dev advanced while checking images; skipping alias publication.")
        return
    for image, auth, body, content_type in manifests:
        registry, repository = image.split("/", 1)
        url = f"https://{registry}/v2/{repository}/manifests/dev"
        with request(url, "PUT", {**auth, "Content-Type": content_type}, body) as response:
            digest = response.headers.get("Docker-Content-Digest")
        with request(url, headers={**auth, "Accept": ACCEPT}) as response:
            if hashlib.sha256(response.read()).digest() != hashlib.sha256(body).digest():
                raise RuntimeError(f"Dev alias verification failed for {image}")
        print(f"Published {image}:dev ({digest})", flush=True)
    prune()
    Path("dev-release.env").write_text(f"DEV_PUBLISHED=true\nDEV_IMAGE_TAG={image_tag()}\n")


def kubectl_binary():
    version = os.environ.get("KUBECTL_VERSION", "v1.31.14")
    if not re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+", version):
        raise ValueError("KUBECTL_VERSION must be a release version")
    architecture = {"x86_64": "amd64", "aarch64": "arm64"}[os.uname().machine]
    url = f"https://dl.k8s.io/release/{version}/bin/linux/{architecture}/kubectl"
    with request(url + ".sha256") as response:
        expected = response.read().decode().strip()
    target = Path(".cache") / f"kubectl-{version}-{architecture}"
    target.parent.mkdir(exist_ok=True)
    if not target.exists() or hashlib.sha256(target.read_bytes()).hexdigest() != expected:
        with request(url) as response:
            content = response.read()
        if hashlib.sha256(content).hexdigest() != expected:
            raise RuntimeError("kubectl checksum mismatch")
        target.write_bytes(content)
    target.chmod(0o700)
    return str(target.resolve())


def deploy():
    if os.environ.get("DEV_PUBLISHED") != "true" or not current():
        print("This pipeline is not the current published dev release; skipping rollout.")
        return
    namespace = os.environ["KUBE_NAMESPACE"]
    kubeconfig = os.environ["KUBE_CONFIG"]
    if not Path(kubeconfig).is_file():
        raise ValueError("KUBE_CONFIG must be a GitLab file variable containing a kubeconfig")
    context = os.environ.get("KUBE_CONTEXT")
    command = [kubectl_binary(), "--kubeconfig", kubeconfig, "--namespace", namespace]
    if context:
        command += ["--context", context]
    images = set(image_names())
    workloads = {}
    for line in Path(".gitlab/ci/workloads.txt").read_text().splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        resource, container, component = line.split()
        image = os.environ["CI_REGISTRY_IMAGE"] + ("" if component == "app" else f"/{component}")
        if image not in images or not re.fullmatch(r"(deployment|statefulset)/[a-z0-9][a-z0-9.-]*", resource):
            raise ValueError("Invalid workload or image mapping")
        workloads.setdefault(resource, {})[container] = f"{image}:{image_tag()}"
    if not workloads:
        raise ValueError("Configure .gitlab/ci/workloads.txt before enabling deployment")
    for resource, containers in workloads.items():
        existing = json.loads(subprocess.check_output([*command, "get", resource, "-o", "json"]))
        names = {item["name"] for item in existing["spec"]["template"]["spec"]["containers"]}
        if not set(containers) <= names:
            raise ValueError(f"Container mapping does not match {resource}")
    if not current():
        print("Dev advanced during preflight; skipping rollout.")
        return
    for resource, containers in workloads.items():
        subprocess.run([*command, "set", "image", resource,
                        *[f"{name}={image}" for name, image in containers.items()]], check=True)
    for resource in workloads:
        subprocess.run([*command, "rollout", "status", resource, "--timeout=300s"], check=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["publish", "deploy", "cleanup"])
    args = parser.parse_args()
    {"publish": publish, "deploy": deploy, "cleanup": prune}[args.action]()
