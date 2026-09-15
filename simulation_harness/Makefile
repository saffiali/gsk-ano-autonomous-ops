# GSK Autonomous Network Operations (ANO) — command surface.
#
# This is the FROZEN contract C6 from PROJECT.md. Every target is defined here
# up front so that no later milestone needs to edit this file.
#
#   make demo           the full demo            (module provided by M6)
#   make eval           the full evaluation      (module provided by M5)
#   make test           unit tests               (all milestones)
#   make e2e            opaque-box suite         (E2E track)
#   make validate-iac   offline IaC validation   (M6)
#   make check-imports  stdlib-only + vendor scan (M1)
#   make verify-offline demo + eval + IaC re-run under real network isolation
#
# `make demo` and `make eval` are the two documented commands the acceptance
# criteria require.
#
# HONESTY RULE: a target whose owning milestone has not landed exits NON-ZERO
# with a "not yet implemented" message. It never reports a fake success.

PY ?= python3

# Determinism, belt and braces: PYTHONHASHSEED=0 removes the per-process hash
# salt. The code must not depend on this (it uses hashlib, never builtin
# hash()), but setting it means an accidental regression shows up as a
# consistent result here and an inconsistent one in review, rather than the
# other way round.
export PYTHONHASHSEED = 0

# Demo parameters.
SCENARIO ?= db_contention_cascade
SEED     ?= 1234

# Evaluation parameters. AC-03 requires at least 5 distinct seeds.
SEEDS    ?= 5
OUT      ?= artifacts/results.json

.DEFAULT_GOAL := help
.PHONY: help demo eval test e2e validate-iac check-imports verify-offline \
        verify-offline-inner clean

# $(call require_module,<module>,<owning milestone>)
# Exits non-zero with an explanatory message if <module> is not importable.
define require_module
@$(PY) -c "import importlib.util, sys; sys.exit(0 if importlib.util.find_spec('$(1)') else 1)" >/dev/null 2>&1 || { \
	echo ""; \
	echo "make $@: NOT YET IMPLEMENTED"; \
	echo "  This target delegates to '$(1)', which is delivered by milestone $(2)"; \
	echo "  and has not landed yet."; \
	echo ""; \
	echo "  Exiting non-zero deliberately: reporting success for a component"; \
	echo "  that does not exist would be worse than failing."; \
	echo ""; \
	exit 1; \
}
endef

help:
	@echo "GSK ANO - command surface (contract C6)"
	@echo ""
	@echo "  make demo            run the full demo offline      [scenario=$(SCENARIO) seed=$(SEED)]"
	@echo "  make eval            run the evaluation harness     [seeds=$(SEEDS) out=$(OUT)]"
	@echo "  make test            run the unit test suite"
	@echo "  make e2e             run the opaque-box E2E suite"
	@echo "  make validate-iac    validate the IaC offline"
	@echo "  make check-imports   stdlib-only imports + no third-party vendor"
	@echo "  make verify-offline  re-run demo + eval + IaC inside 'unshare -rn'"
	@echo "  make clean           remove generated artifacts"
	@echo ""
	@echo "The two documented commands required by the acceptance criteria are"
	@echo "'make demo' and 'make eval'. See README.md."

# --- the two documented commands -------------------------------------------

demo:
	$(call require_module,ano.demo.run,M6)
	$(PY) -m ano.demo.run --scenario $(SCENARIO) --seed $(SEED)

eval:
	$(call require_module,harness.run,M5)
	$(PY) -m harness.run --seeds $(SEEDS) --out $(OUT)

# --- verification -----------------------------------------------------------

test:
	$(PY) -m unittest discover -s tests

e2e:
	$(call require_module,e2e.run,the E2E track)
	$(PY) -m e2e.run

validate-iac:
	@test -f iac/validate_iac.py || { \
		echo ""; \
		echo "make validate-iac: NOT YET IMPLEMENTED"; \
		echo "  This target runs 'iac/validate_iac.py', which is delivered by"; \
		echo "  milestone M6 and has not landed yet."; \
		echo ""; \
		echo "  Exiting non-zero deliberately: reporting success for a component"; \
		echo "  that does not exist would be worse than failing."; \
		echo ""; \
		exit 1; \
	}
	$(PY) iac/validate_iac.py

check-imports:
	$(PY) tools/check_imports.py

# --- offline proof ----------------------------------------------------------
#
# This machine currently HAS network access. "Runs offline" must therefore be
# demonstrated under genuine isolation rather than asserted: `unshare -rn`
# creates an empty network namespace (no interfaces, no DNS), and the demo, the
# evaluation and the IaC validation are re-run inside it. If any of them has
# acquired a hidden network dependency, this target fails.

verify-offline:
	@command -v unshare >/dev/null 2>&1 || { \
		echo "make verify-offline: 'unshare' is not available on this machine."; \
		echo "  It is required to create the empty network namespace that makes"; \
		echo "  the offline claim testable rather than asserted."; \
		exit 1; \
	}
	@echo "[verify-offline] entering an empty network namespace (unshare -rn)"
	@unshare -rn $(MAKE) --no-print-directory verify-offline-inner

verify-offline-inner:
	@echo "[verify-offline] confirming the namespace really has no network"
	@$(PY) -c "import socket, sys; s = socket.socket(); s.settimeout(2); rc = s.connect_ex(('8.8.8.8', 53)); print('  probe 8.8.8.8:53 ->', 'REACHABLE - isolation FAILED' if rc == 0 else 'unreachable (errno %d) - isolation confirmed' % rc); sys.exit(1 if rc == 0 else 0)"
	@echo "[verify-offline] running: make demo"
	@$(MAKE) --no-print-directory demo
	@echo "[verify-offline] running: make eval"
	@$(MAKE) --no-print-directory eval
	@echo "[verify-offline] running: make validate-iac"
	@$(MAKE) --no-print-directory validate-iac
	@echo ""
	@echo "[verify-offline] PASS - demo, evaluation and IaC validation all"
	@echo "                 completed with no network access."

clean:
	rm -rf artifacts
	find . -name '__pycache__' -type d -prune -exec rm -rf {} +
	find . -name '*.pyc' -delete
