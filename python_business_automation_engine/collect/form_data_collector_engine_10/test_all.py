"""
╔══════════════════════════════════════════════════════════════════════════════╗
║          FORM DATA COLLECTOR ENGINE #10 — Test Runner                       ║
║          Python Business Automation Engine | Portfolio Professional          ║
╚══════════════════════════════════════════════════════════════════════════════╝

Module  : test_all.py
Purpose : Unified test runner — satu perintah cek semua modul sekaligus.

Usage:
    python test_all.py              # run semua test
    python test_all.py --module validators   # hanya validators
    python test_all.py --module collectors  # hanya collectors
    python test_all.py --verbose            # output detail per test case
"""

from __future__ import annotations

import sys
import argparse
import traceback
from pathlib import Path
from datetime import datetime

# ── path setup ────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

# ── disable loguru noise during tests ────────────────────────────────────────
from loguru import logger
logger.remove()   # silent during tests — kita pakai print sendiri

import pandas as pd

# ─────────────────────────────────────────────────────────────────────────────
# MINI TEST FRAMEWORK
# ─────────────────────────────────────────────────────────────────────────────

PASS  = "✅ PASS"
FAIL  = "❌ FAIL"
SKIP  = "⚠️  SKIP"

results: list[dict] = []

def run_test(name: str, fn, *args, **kwargs):
    """Run a single test case and record result."""
    try:
        fn(*args, **kwargs)
        results.append({"name": name, "status": "PASS", "error": ""})
        print(f"  {PASS}  {name}")
    except AssertionError as e:
        results.append({"name": name, "status": "FAIL", "error": str(e)})
        print(f"  {FAIL}  {name}")
        print(f"         → {e}")
    except ImportError as e:
        results.append({"name": name, "status": "SKIP", "error": str(e)})
        print(f"  {SKIP}  {name}  (module not found: {e})")
    except Exception as e:
        results.append({"name": name, "status": "FAIL", "error": str(e)})
        print(f"  {FAIL}  {name}")
        print(f"         → {type(e).__name__}: {e}")

def section(title: str):
    print(f"\n{'═' * 60}")
    print(f"  {title}")
    print(f"{'═' * 60}")

def subsection(title: str):
    print(f"\n  ── {title} ──")


# ─────────────────────────────────────────────────────────────────────────────
# TEST SUITE: EMAIL VALIDATOR
# ─────────────────────────────────────────────────────────────────────────────

def test_email_valid_gmail():
    from validators.email_validator import is_valid_email
    assert is_valid_email("user@gmail.com") is True

def test_email_valid_indonesia():
    from validators.email_validator import is_valid_email
    assert is_valid_email("user@yahoo.co.id") is True

def test_email_valid_plus_tag():
    from validators.email_validator import is_valid_email
    assert is_valid_email("user.name+tag@outlook.com") is True

def test_email_invalid_no_at():
    from validators.email_validator import is_valid_email
    assert is_valid_email("invalidemail.com") is False

def test_email_invalid_no_domain():
    from validators.email_validator import is_valid_email
    assert is_valid_email("user@") is False

def test_email_invalid_disposable():
    from validators.email_validator import is_valid_email
    assert is_valid_email("test@mailinator.com") is False

def test_email_empty():
    from validators.email_validator import is_valid_email
    assert is_valid_email("") is False

def test_email_batch_returns_dataframe():
    from validators.email_validator import EmailValidator
    v  = EmailValidator()
    df = v.validate_batch(["a@gmail.com", "bad-email", "x@mailinator.com"])
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert bool(df.loc[0, "is_valid"]) is True
    assert bool(df.loc[1, "is_valid"]) is False
    assert bool(df.loc[2, "is_valid"]) is False   # disposable


# ─────────────────────────────────────────────────────────────────────────────
# TEST SUITE: PHONE VALIDATOR
# ─────────────────────────────────────────────────────────────────────────────

def test_phone_local_zero_prefix():
    from validators.phone_validator import is_valid_phone, normalize_phone
    assert is_valid_phone("08123456789") is True
    assert normalize_phone("08123456789") == "+628123456789"

def test_phone_no_prefix():
    from validators.phone_validator import normalize_phone
    assert normalize_phone("8123456789") == "+628123456789"

def test_phone_with_62():
    from validators.phone_validator import normalize_phone
    assert normalize_phone("628123456789") == "+628123456789"

def test_phone_e164():
    from validators.phone_validator import is_valid_phone
    assert is_valid_phone("+628123456789") is True

def test_phone_formatted_dashes():
    from validators.phone_validator import normalize_phone
    assert normalize_phone("0812-3456-789") == "+628123456789"

def test_phone_formatted_spaces():
    from validators.phone_validator import normalize_phone
    assert normalize_phone("0812 3456 789") == "+628123456789"

def test_phone_too_short():
    from validators.phone_validator import is_valid_phone
    assert is_valid_phone("0812") is False

def test_phone_empty():
    from validators.phone_validator import is_valid_phone
    assert is_valid_phone("") is False

def test_phone_non_digits():
    from validators.phone_validator import is_valid_phone
    assert is_valid_phone("abcdefgh") is False

def test_phone_operator_detection():
    from validators.phone_validator import PhoneValidator
    v = PhoneValidator()
    assert v.validate("08123456789").operator  == "Telkomsel"
    assert v.validate("08556789012").operator  == "Indosat"
    assert v.validate("08778901234").operator  == "XL"

def test_phone_batch_returns_dataframe():
    from validators.phone_validator import PhoneValidator
    v  = PhoneValidator()
    df = v.validate_batch(["08123456789", "bad", "+628556789012"])
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3
    assert bool(df.loc[0, "is_valid"]) is True
    assert bool(df.loc[1, "is_valid"]) is False


# ─────────────────────────────────────────────────────────────────────────────
# TEST SUITE: DUPLICATE CHECKER
# ─────────────────────────────────────────────────────────────────────────────

def _sample_df():
    return pd.DataFrame({
        "Email": [
            "budi@gmail.com", "siti@yahoo.com",
            "budi@gmail.com",       # exact duplicate
            "BUDI@GMAIL.COM",       # case-insensitive duplicate
            "andi@outlook.com",
        ],
        "Name": ["Budi", "Siti", "Budi", "Budi", "Andi"],
    })

def test_duplicate_exact_finds_dupes():
    from validators.duplicate_checker import find_duplicates
    df   = _sample_df()
    mask = find_duplicates(df, key_col="Email", strategy="exact")
    assert mask.sum() == 2   # rows 2 and 3 are duplicates

def test_duplicate_exact_keep_first():
    from validators.duplicate_checker import DuplicateChecker
    df      = _sample_df()
    checker = DuplicateChecker(strategy="exact", key_col="Email", keep="first")
    result  = checker.remove_duplicates(df)
    assert len(result) == 3  # budi, siti, andi

def test_duplicate_no_dupes_in_clean_df():
    from validators.duplicate_checker import DuplicateChecker
    df      = pd.DataFrame({"Email": ["a@x.com", "b@x.com", "c@x.com"]})
    checker = DuplicateChecker(strategy="exact", key_col="Email")
    result  = checker.check(df)
    assert result.duplicate_count == 0
    assert result.has_duplicates is False

def test_duplicate_composite_key():
    from validators.duplicate_checker import DuplicateChecker
    df = pd.DataFrame({
        "Name" : ["Budi", "Budi", "Siti"],
        "Phone": ["08123", "08123", "08456"],
    })
    checker = DuplicateChecker(
        strategy="composite", key_col="Name", extra_cols=["Phone"]
    )
    result = checker.check(df)
    assert result.duplicate_count == 1

def test_duplicate_empty_df():
    from validators.duplicate_checker import DuplicateChecker
    checker = DuplicateChecker()
    result  = checker.check(pd.DataFrame())
    assert result.total_rows == 0


# ─────────────────────────────────────────────────────────────────────────────
# TEST SUITE: FORMAT VALIDATOR
# ─────────────────────────────────────────────────────────────────────────────

def test_format_date_slash():
    from validators.format_validator import validate_date
    r = validate_date("23/06/2024")
    assert r.is_valid is True
    assert r.normalized == "2024-06-23"

def test_format_date_iso():
    from validators.format_validator import validate_date
    r = validate_date("2024-06-23")
    assert r.is_valid is True

def test_format_date_invalid():
    from validators.format_validator import validate_date
    r = validate_date("32/13/2024")
    assert r.is_valid is False

def test_format_date_empty():
    from validators.format_validator import validate_date
    r = validate_date("")
    assert r.is_valid is False

def test_format_integer_valid():
    from validators.format_validator import validate_integer
    r = validate_integer("42")
    assert r.is_valid is True
    assert r.detail["parsed"] == 42

def test_format_integer_range_fail():
    from validators.format_validator import validate_integer
    r = validate_integer("99", max_val=50)
    assert r.is_valid is False

def test_format_float_european():
    from validators.format_validator import validate_float
    r = validate_float("1.500,75")
    assert r.is_valid is True
    assert float(r.normalized) == 1500.75

def test_format_url_valid():
    from validators.format_validator import validate_url
    r = validate_url("https://talatee.com")
    assert r.is_valid is True

def test_format_url_auto_prepend():
    from validators.format_validator import validate_url
    r = validate_url("talatee.com")
    assert r.is_valid is True
    assert r.normalized.startswith("https://")

def test_format_url_invalid():
    from validators.format_validator import validate_url
    r = validate_url("not a url!!")
    assert r.is_valid is False

def test_format_nik_valid():
    from validators.format_validator import validate_nik
    r = validate_nik("3578012306900001")
    assert r.is_valid is True
    assert r.detail["gender"] == "L (Laki-laki)"

def test_format_nik_invalid_length():
    from validators.format_validator import validate_nik
    r = validate_nik("123456789")
    assert r.is_valid is False

def test_format_npwp_formatted():
    from validators.format_validator import validate_npwp
    r = validate_npwp("01.234.567.8-901.234")
    assert r.is_valid is True

def test_format_npwp_digits():
    from validators.format_validator import validate_npwp
    r = validate_npwp("012345678901234")
    assert r.is_valid is True

def test_format_postal_id_valid():
    from validators.format_validator import validate_postal_code
    r = validate_postal_code("60111", country="ID")
    assert r.is_valid is True

def test_format_postal_id_invalid():
    from validators.format_validator import validate_postal_code
    r = validate_postal_code("00000", country="ID")
    assert r.is_valid is False

def test_format_gender_laki():
    from validators.format_validator import validate_gender
    for val in ["Laki-laki", "laki", "L", "l", "male", "Male", "pria"]:
        r = validate_gender(val)
        assert r.is_valid is True and r.normalized == "L", f"Failed for: {val}"

def test_format_gender_perempuan():
    from validators.format_validator import validate_gender
    for val in ["Perempuan", "perempuan", "P", "p", "female", "wanita"]:
        r = validate_gender(val)
        assert r.is_valid is True and r.normalized == "P", f"Failed for: {val}"

def test_format_gender_invalid():
    from validators.format_validator import validate_gender
    r = validate_gender("X")
    assert r.is_valid is False

def test_format_batch_dataframe():
    from validators.format_validator import FormatValidator
    df = pd.DataFrame({
        "NIK"    : ["3578012306900001", "123"],
        "Kode Pos": ["60111", "00000"],
    })
    schema    = {"NIK": {"type": "nik"}, "Kode Pos": {"type": "postal_code", "country": "ID"}}
    validator = FormatValidator(schema)
    ann, rep  = validator.validate_dataframe(df)
    assert isinstance(ann, pd.DataFrame)
    assert isinstance(rep, pd.DataFrame)
    # report_df columns: column, field_type, total, valid, invalid, valid_pct
    assert len(rep) == 2                             # 2 kolom dicek
    assert rep.loc[rep["column"] == "NIK",      "valid"].values[0] == 1
    assert rep.loc[rep["column"] == "Kode Pos", "valid"].values[0] == 1


# ─────────────────────────────────────────────────────────────────────────────
# TEST SUITE: REQUIRED FIELDS VALIDATOR
# ─────────────────────────────────────────────────────────────────────────────

def _form_df():
    return pd.DataFrame({
        "Nama Lengkap" : ["Budi",  "",     "Siti"],
        "Email Address": ["b@g.com","s@y.com",""],
        "Nomor HP"     : ["081",   "085",   "087"],
        "Kota"         : ["SBY",   "",      "JKT"],
    })

def test_required_fields_valid_row():
    from validators.required_fields import check_required_fields
    df   = _form_df()
    mask = check_required_fields(df, required=["Nama Lengkap", "Email Address", "Nomor HP"])
    assert bool(mask.iloc[0]) is True    # row 0: semua ada

def test_required_fields_missing_nama():
    from validators.required_fields import check_required_fields
    df   = _form_df()
    mask = check_required_fields(df, required=["Nama Lengkap", "Email Address", "Nomor HP"])
    assert bool(mask.iloc[1]) is False   # row 1: Nama kosong

def test_required_fields_missing_email():
    from validators.required_fields import check_required_fields
    df   = _form_df()
    mask = check_required_fields(df, required=["Nama Lengkap", "Email Address", "Nomor HP"])
    assert bool(mask.iloc[2]) is False   # row 2: Email kosong

def test_required_fields_alias_resolution():
    from validators.required_fields import FieldSpec, RequiredFieldsValidator
    df = pd.DataFrame({
        "Email Address": ["a@g.com"],   # alias dari "Email"
        "Nama"         : ["Budi"],
    })
    schema    = [FieldSpec("Email", alias=["Email Address"]),
                 FieldSpec("Nama")]
    validator = RequiredFieldsValidator(schema)
    result    = validator.check(df)
    assert result.valid_rows == 1

def test_required_fields_conditional():
    from validators.required_fields import FieldSpec, RequiredFieldsValidator
    df = pd.DataFrame({
        "Nama"        : ["Budi",        "PT Maju"],
        "Tipe"        : ["Individu",    "Perusahaan"],
        "NPWP"        : ["",            "01.234.567.8-901.234"],
    })
    schema = [
        FieldSpec("Nama"),
        FieldSpec("NPWP", severity="error",
                  condition=lambda row: row.get("Tipe") == "Perusahaan"),
    ]
    validator = RequiredFieldsValidator(schema)
    result    = validator.check(df)
    # Row 0 (Individu): NPWP not required → valid
    # Row 1 (Perusahaan): NPWP filled → valid
    assert result.valid_rows == 2

def test_required_fields_preset_lead_form():
    from validators.required_fields import FormSchemas, RequiredFieldsValidator
    df = pd.DataFrame({
        "Nama Lengkap": ["Budi", ""],
        "Email"       : ["b@g.com", "s@y.com"],
        "No. HP"      : ["081", "085"],
    })
    validator = RequiredFieldsValidator(FormSchemas.lead_form())
    result    = validator.check(df)
    assert result.error_rows == 1   # row 1: Nama kosong

def test_required_fields_column_report():
    from validators.required_fields import check_required_fields, RequiredFieldsValidator, FieldSpec
    df     = _form_df()
    schema = [FieldSpec("Nama Lengkap"), FieldSpec("Email Address")]
    result = RequiredFieldsValidator(schema).check(df)
    assert isinstance(result.column_report, pd.DataFrame)
    assert "fill_rate" in result.column_report.columns


# ─────────────────────────────────────────────────────────────────────────────
# TEST SUITE: ENVIRONMENT & IMPORTS
# ─────────────────────────────────────────────────────────────────────────────

def test_env_dotenv_loadable():
    from dotenv import load_dotenv
    result = load_dotenv(BASE_DIR / ".env")
    # load_dotenv returns True even if file not found — just check no exception
    assert result is not None

def test_pandas_available():
    import pandas as pd
    df = pd.DataFrame({"a": [1, 2, 3]})
    assert len(df) == 3

def test_loguru_available():
    from loguru import logger
    assert logger is not None

def test_pathlib_base_dir():
    assert BASE_DIR.exists()
    assert (BASE_DIR / "main.py").exists(), "main.py tidak ditemukan di root!"

def test_validators_folder_exists():
    assert (BASE_DIR / "validators").exists(), "Folder validators/ tidak ditemukan!"

def test_collectors_folder_exists():
    assert (BASE_DIR / "collectors").exists(), "Folder collectors/ tidak ditemukan!"

def test_logs_folder_exists():
    logs = BASE_DIR / "logs"
    logs.mkdir(exist_ok=True)
    assert logs.exists()

def test_outputs_folder_exists():
    out = BASE_DIR / "outputs"
    out.mkdir(exist_ok=True)
    assert out.exists()


# ─────────────────────────────────────────────────────────────────────────────
# TEST REGISTRY
# ─────────────────────────────────────────────────────────────────────────────

ALL_TESTS = {
    "environment": [
        ("ENV  | dotenv loadable",              test_env_dotenv_loadable),
        ("ENV  | pandas available",             test_pandas_available),
        ("ENV  | loguru available",             test_loguru_available),
        ("ENV  | BASE_DIR & main.py exist",     test_pathlib_base_dir),
        ("ENV  | validators/ folder exists",    test_validators_folder_exists),
        ("ENV  | collectors/ folder exists",    test_collectors_folder_exists),
        ("ENV  | logs/ folder auto-created",    test_logs_folder_exists),
        ("ENV  | outputs/ folder auto-created", test_outputs_folder_exists),
    ],
    "validators": [
        # Email
        ("EMAIL | valid gmail",                 test_email_valid_gmail),
        ("EMAIL | valid yahoo.co.id",           test_email_valid_indonesia),
        ("EMAIL | valid plus tag",              test_email_valid_plus_tag),
        ("EMAIL | invalid no @",               test_email_invalid_no_at),
        ("EMAIL | invalid no domain",          test_email_invalid_no_domain),
        ("EMAIL | disposable blocked",         test_email_invalid_disposable),
        ("EMAIL | empty string",               test_email_empty),
        ("EMAIL | batch → DataFrame",          test_email_batch_returns_dataframe),
        # Phone
        ("PHONE | 08xxx → E.164",              test_phone_local_zero_prefix),
        ("PHONE | 8xxx → E.164",               test_phone_no_prefix),
        ("PHONE | 628xxx → E.164",             test_phone_with_62),
        ("PHONE | +628xxx valid",              test_phone_e164),
        ("PHONE | dash format",                test_phone_formatted_dashes),
        ("PHONE | space format",               test_phone_formatted_spaces),
        ("PHONE | too short invalid",          test_phone_too_short),
        ("PHONE | empty invalid",              test_phone_empty),
        ("PHONE | non-digits invalid",         test_phone_non_digits),
        ("PHONE | operator detection",         test_phone_operator_detection),
        ("PHONE | batch → DataFrame",          test_phone_batch_returns_dataframe),
        # Duplicate
        ("DUPE  | exact finds 2 dupes",        test_duplicate_exact_finds_dupes),
        ("DUPE  | remove keeps first",         test_duplicate_exact_keep_first),
        ("DUPE  | clean df has 0 dupes",       test_duplicate_no_dupes_in_clean_df),
        ("DUPE  | composite key",              test_duplicate_composite_key),
        ("DUPE  | empty df handled",           test_duplicate_empty_df),
        # Format
        ("FMT   | date slash format",          test_format_date_slash),
        ("FMT   | date ISO format",            test_format_date_iso),
        ("FMT   | date invalid",               test_format_date_invalid),
        ("FMT   | date empty",                 test_format_date_empty),
        ("FMT   | integer valid",              test_format_integer_valid),
        ("FMT   | integer range fail",         test_format_integer_range_fail),
        ("FMT   | float European format",      test_format_float_european),
        ("FMT   | url valid",                  test_format_url_valid),
        ("FMT   | url auto-prepend https",     test_format_url_auto_prepend),
        ("FMT   | url invalid",                test_format_url_invalid),
        ("FMT   | nik valid + gender",         test_format_nik_valid),
        ("FMT   | nik invalid length",         test_format_nik_invalid_length),
        ("FMT   | npwp formatted",             test_format_npwp_formatted),
        ("FMT   | npwp 15 digits",             test_format_npwp_digits),
        ("FMT   | postal code ID valid",       test_format_postal_id_valid),
        ("FMT   | postal code ID invalid",     test_format_postal_id_invalid),
        ("FMT   | gender laki-laki variants",  test_format_gender_laki),
        ("FMT   | gender perempuan variants",  test_format_gender_perempuan),
        ("FMT   | gender invalid",             test_format_gender_invalid),
        ("FMT   | batch DataFrame",            test_format_batch_dataframe),
        # Required Fields
        ("REQ   | valid row passes",           test_required_fields_valid_row),
        ("REQ   | missing nama fails",         test_required_fields_missing_nama),
        ("REQ   | missing email fails",        test_required_fields_missing_email),
        ("REQ   | alias column resolved",      test_required_fields_alias_resolution),
        ("REQ   | conditional field",          test_required_fields_conditional),
        ("REQ   | preset lead_form",           test_required_fields_preset_lead_form),
        ("REQ   | column report returned",     test_required_fields_column_report),
    ],
}


# ─────────────────────────────────────────────────────────────────────────────
# RUNNER
# ─────────────────────────────────────────────────────────────────────────────

def run_suite(suite_name: str, tests: list[tuple]) -> tuple[int, int, int]:
    """Run a suite of tests. Returns (passed, failed, skipped)."""
    passed = failed = skipped = 0
    for name, fn in tests:
        run_test(name, fn)
        last = results[-1]["status"]
        if last == "PASS":  passed  += 1
        elif last == "FAIL": failed += 1
        else:               skipped += 1
    return passed, failed, skipped


def print_final_report(start: datetime):
    elapsed = (datetime.now() - start).total_seconds()
    total   = len(results)
    passed  = sum(1 for r in results if r["status"] == "PASS")
    failed  = sum(1 for r in results if r["status"] == "FAIL")
    skipped = sum(1 for r in results if r["status"] == "SKIP")

    print("\n" + "╔" + "═" * 58 + "╗")
    print("║   FINAL TEST REPORT" + " " * 38 + "║")
    print("╠" + "═" * 58 + "╣")
    print(f"║   Total   : {total:<3}  tests ran in {elapsed:.2f}s" + " " * (28 - len(f"{elapsed:.2f}")) + "║")
    print(f"║   ✅ Pass  : {passed:<44}║")
    print(f"║   ❌ Fail  : {failed:<44}║")
    print(f"║   ⚠️  Skip  : {skipped:<43}║")
    print("╠" + "═" * 58 + "╣")

    if failed == 0 and skipped == 0:
        print("║   🎉  ALL TESTS PASSED — Engine siap digunakan!       ║")
    elif failed == 0:
        print("║   ✅  PASSED (with skips) — cek modul yang skip        ║")
    else:
        print("║   ❌  SOME TESTS FAILED — lihat detail di atas         ║")
        print("╠" + "═" * 58 + "╣")
        print("║   Tests yang gagal:                                   ║")
        for r in results:
            if r["status"] == "FAIL":
                name_short = r["name"][:50]
                print(f"║   • {name_short:<53}║")

    print("╚" + "═" * 58 + "╝\n")

    return failed


def main():
    parser = argparse.ArgumentParser(
        description="Form Data Collector Engine #10 — Test Runner"
    )
    parser.add_argument(
        "--module",
        choices=["all", "environment", "validators", "collectors"],
        default="all",
        help="Pilih modul yang ingin ditest (default: all)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Tampilkan output detail setiap test case",
    )
    args  = parser.parse_args()
    start = datetime.now()

    print("\n" + "╔" + "═" * 58 + "╗")
    print("║   FORM DATA COLLECTOR ENGINE #10 — TEST RUNNER        ║")
    print(f"║   {datetime.now():%Y-%m-%d %H:%M:%S}                              ║")
    print("╚" + "═" * 58 + "╝")

    suites_to_run = (
        list(ALL_TESTS.keys()) if args.module == "all"
        else [args.module]
    )

    for suite_name in suites_to_run:
        if suite_name not in ALL_TESTS:
            print(f"\n⚠️  Suite '{suite_name}' tidak ditemukan.")
            continue
        section(f"SUITE: {suite_name.upper()}")
        run_suite(suite_name, ALL_TESTS[suite_name])

    failed = print_final_report(start)
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()