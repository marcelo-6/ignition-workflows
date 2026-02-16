set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

themes_dir := "/usr/local/bin/ignition/data/modules/com.inductiveautomation.perspective/themes"
import_line := "@import \"./custom/index.css\";"
conv_commit_regex := "^(feat|fix|docs|style|refactor|perf|test|build|ci|chore)(\\([a-zA-Z0-9._/-]+\\))?(!)?: .+"
zensical_image := "zensical/zensical:latest"

default:
	@just --list

doctor:
	@echo "Checking local tools..."
	@command -v just >/dev/null && echo "  just: ok" || echo "  just: missing"
	@command -v git >/dev/null && echo "  git: ok" || echo "  git: missing"
	@command -v docker >/dev/null && echo "  docker: ok" || echo "  docker: missing"
	@command -v docker compose >/dev/null && echo "  docker compose: ok" || echo "  docker compose: missing"
	@command -v cargo >/dev/null && echo "  cargo: ok" || echo "  cargo: missing"
	@command -v git-cliff >/dev/null && echo "  git-cliff: ok" || echo "  git-cliff: missing (run: just install-git-cliff)"

install-git-cliff:
	@if command -v git-cliff >/dev/null 2>&1; then \
		echo "git-cliff already installed"; \
		exit 0; \
	fi
	@if ! command -v cargo >/dev/null 2>&1; then \
		echo "cargo is required to install git-cliff"; \
		echo "Install Rust first: https://rustup.rs/"; \
		exit 1; \
	fi
	cargo install git-cliff

changelog:
	git cliff -o CHANGELOG.md

changelog-unreleased:
	git cliff --unreleased

bump:
	git cliff --bump --unreleased

release-assets version gateway_name="MainGateway":
	mkdir -p dist
	zip -r "dist/projects-{{gateway_name}}-{{version}}.zip" projects
	@if ls -t "projects/gw-backups"/*"${gateway_name}"* >/dev/null 2>&1; then \
		cp "$(ls -t "projects/gw-backups"/*"${gateway_name}"* | head -1)" dist/; \
	fi
	@if ls -t "projects/tag-backups"/*"${gateway_name}"* >/dev/null 2>&1; then \
		cp "$(ls -t "projects/tag-backups"/*"${gateway_name}"* | head -1)" dist/; \
	fi
	@cd dist && sha256sum * > SHA256SUMS
	@echo "Release artifacts ready in ./dist"

commit message:
	git add -A
	git commit -m "{{message}}"

# Docker related

# Remove all images, volumes, and orphans from this compose project
remove-images:
	docker compose down --rmi all --volumes --remove-orphans

# Build without using cache and always pull latest base images
build-nocache:
	docker compose build --no-cache --pull

# Bring the stack up in detached mode
up:
	docker compose up -d

# Bring the stack down
down:
	docker compose down

# Full clean rebuild + up
rebuild:
	just remove-images
	just build-nocache
	just up

# Documentation (Zensical via Docker https://zensical.org)

# Scaffold a Zensical site into a target path 
docs-new target=".":
	docker run --rm -it \
		-u "$(id -u):$(id -g)" \
		-v "$PWD:/work" \
		-w /work \
		{{zensical_image}} \
		new "{{target}}"

# Serve docs locally at http://localhost:8000 
docs-serve port="8000":
	docker run --rm -it \
		-p "{{port}}:8000" \
		-v "$PWD:/docs" \
		{{zensical_image}}

# Build static docs into ./site
docs-build:
	docker run --rm -it \
		-u "$(id -u):$(id -g)" \
		-v "$PWD:/docs" \
		{{zensical_image}} \
		build --clean
