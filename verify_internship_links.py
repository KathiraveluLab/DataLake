"""Verify live HTTP status codes for Alaska software and game-dev internship portals
and generate c:/DataLake/alaska_software_internships.md using only verified HTTP 200 links.
"""

from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class InternshipResource:
    category: str
    organization: str
    location: str
    focus: str
    url: str


CANDIDATE_RESOURCES: list[InternshipResource] = [
    # 1. University of Alaska System and Research Institutes
    InternshipResource(
        category="University of Alaska System and Research Institutes",
        organization="University of Alaska Careers Portal (UAA, UAF, UAS, GI, ASF, ACEP)",
        location="Anchorage, Fairbanks, Juneau, AK",
        focus="Student software developer, research computing, and IT assistant positions across the UA system.",
        url="https://careers.alaska.edu/",
    ),
    InternshipResource(
        category="University of Alaska System and Research Institutes",
        organization="UAA / UAF Handshake Student Portal (CSCE A395 Credit Path)",
        location="Anchorage and Fairbanks, AK",
        focus="Campus-vetted local Alaska employer internships in software engineering, databases, and IT.",
        url="https://uaa.joinhandshake.com/",
    ),
    InternshipResource(
        category="University of Alaska System and Research Institutes",
        organization="University of Alaska Anchorage (UAA)",
        location="Anchorage, AK",
        focus="Computer Science and Engineering internship coordination (CSCE A395/A495) and departmental research.",
        url="https://www.uaa.alaska.edu/",
    ),
    InternshipResource(
        category="University of Alaska System and Research Institutes",
        organization="University of Alaska Fairbanks (UAF)",
        location="Fairbanks, AK",
        focus="Undergraduate research, computing centers, and software/IT student employment.",
        url="https://www.uaf.edu/",
    ),
    InternshipResource(
        category="University of Alaska System and Research Institutes",
        organization="UAF Alaska Center for Energy and Power (ACEP)",
        location="Fairbanks and Anchorage, AK",
        focus="Paid 10-week summer undergraduate research internships (REU/AUSI) in simulation, grid modeling, and data systems.",
        url="https://www.uaf.edu/acep/",
    ),
    InternshipResource(
        category="University of Alaska System and Research Institutes",
        organization="UAF Geophysical Institute (GI) and Alaska Earthquake Center",
        location="Fairbanks, AK",
        focus="Real-time seismic data acquisition, geospatial pipelines, scientific computing, and database infrastructure.",
        url="https://www.gi.alaska.edu/",
    ),
    # 2. State, Municipal, and Federal Public Sector in Alaska
    InternshipResource(
        category="State, Municipal, and Federal Public Sector in Alaska",
        organization="State of Alaska Official Job Board (Workplace Alaska)",
        location="Anchorage, Juneau, Fairbanks, AK",
        focus="Paid College Intern (Levels I to IV) and Student Intern postings in IT, software development, and databases.",
        url="https://www.governmentjobs.com/careers/alaska",
    ),
    InternshipResource(
        category="State, Municipal, and Federal Public Sector in Alaska",
        organization="Municipality of Anchorage",
        location="Anchorage, AK",
        focus="Municipal IT systems, civic software, GIS, and database administration internships.",
        url="https://www.muni.org/",
    ),
    InternshipResource(
        category="State, Municipal, and Federal Public Sector in Alaska",
        organization="USAJOBS (Federal Pathways Internships in Alaska)",
        location="Anchorage, Fairbanks, Juneau, AK",
        focus="Federal agency IT, software, cybersecurity, and data management internships stationed in Alaska.",
        url="https://www.usajobs.gov/",
    ),
    # 3. Telecommunications, Aviation, Engineering, and Healthcare in Alaska
    InternshipResource(
        category="Telecommunications, Aviation, Engineering, and Healthcare",
        organization="GCI Communication Corp.",
        location="Anchorage, AK",
        focus="Software developer, network engineering, cybersecurity, and data analytics summer internships.",
        url="https://www.gci.com/about/careers",
    ),
    InternshipResource(
        category="Telecommunications, Aviation, Engineering, and Healthcare",
        organization="Alaska Airlines / Horizon Air",
        location="Anchorage, AK / Seattle, WA / Hybrid",
        focus="Software engineering, data engineering, cloud platform, and IT internships.",
        url="https://careers.alaskaair.com/",
    ),
    InternshipResource(
        category="Telecommunications, Aviation, Engineering, and Healthcare",
        organization="RESPEC",
        location="Anchorage and Fairbanks, AK",
        focus="Enterprise software engineering, cloud data solutions, and geospatial modeling internships.",
        url="https://www.respec.com/careers/",
    ),
    InternshipResource(
        category="Telecommunications, Aviation, Engineering, and Healthcare",
        organization="DOWL",
        location="Anchorage and Fairbanks, AK",
        focus="GIS software, spatial database, and enterprise IT support internships.",
        url="https://www.dowl.com/careers/",
    ),
    InternshipResource(
        category="Telecommunications, Aviation, Engineering, and Healthcare",
        organization="HDR",
        location="Anchorage, AK",
        focus="Software development, IT, controls, and GIS engineering internships.",
        url="https://www.hdrinc.com/careers",
    ),
    InternshipResource(
        category="Telecommunications, Aviation, Engineering, and Healthcare",
        organization="Alaska Native Tribal Health Consortium (ANTHC)",
        location="Anchorage, AK",
        focus="Health IT systems, clinical database management, and application support internships.",
        url="https://anthc.org/",
    ),
    # 4. Alaska Native Regional Corporations
    InternshipResource(
        category="Alaska Native Regional Corporations",
        organization="Sealaska",
        location="Juneau, AK and Remote",
        focus="Computer science, data analytics, software, and GIS summer internship tracks.",
        url="https://www.sealaska.com/careers/",
    ),
    InternshipResource(
        category="Alaska Native Regional Corporations",
        organization="Doyon, Limited",
        location="Fairbanks and Anchorage, AK",
        focus="Information technology and corporate systems summer internships.",
        url="https://careers.doyon.com/",
    ),
    InternshipResource(
        category="Alaska Native Regional Corporations",
        organization="Bristol Bay Native Corporation (BBNC)",
        location="Anchorage, AK",
        focus="IT apprenticeship and technical career development tracks.",
        url="https://www.bbnc.net/",
    ),
    InternshipResource(
        category="Alaska Native Regional Corporations",
        organization="Arctic Slope Regional Corporation (ASRC)",
        location="Anchorage, AK",
        focus="ASRC Work Study Internship Program across technology, energy, and corporate services.",
        url="https://www.asrc.com/",
    ),
    # 5. Alaska Game Development Community and Remote US Game Studio Tracks
    InternshipResource(
        category="Alaska Game Development and Remote US Studio Database Tracks",
        organization="Alaska Developers Alliance (Alaska Game Developers Community)",
        location="Anchorage, AK (UAA/APU Consortium Library Meetups)",
        focus="Local game jams, indie studio networking, and collaborative portfolio projects.",
        url="https://www.akdevalliance.com/",
    ),
    InternshipResource(
        category="Alaska Game Development and Remote US Studio Database Tracks",
        organization="CakeNeq Games",
        location="Anchorage, AK",
        focus="Independent Alaska game studio developing commercial titles and engaging with the local developer community.",
        url="https://www.cakeneq.com/",
    ),
    InternshipResource(
        category="Alaska Game Development and Remote US Studio Database Tracks",
        organization="E-Line Media",
        location="Remote / Anchorage Partnership (Upper One Games)",
        focus="Co-developer of Never Alone with Cook Inlet Tribal Council and Upper One Games.",
        url="https://www.elinemedia.com/careers/",
    ),
    InternshipResource(
        category="Alaska Game Development and Remote US Studio Database Tracks",
        organization="Riot Games",
        location="Remote-First US",
        focus="Data Engineering Intern and Software Engineering Intern roles covering SQL, distributed data lakes, and backend game telemetry.",
        url="https://www.riotgames.com/en/work-with-us",
    ),
    InternshipResource(
        category="Alaska Game Development and Remote US Studio Database Tracks",
        organization="Roblox",
        location="US (San Mateo / Remote Assessments)",
        focus="Backend Software Engineering Intern and Data Science Intern tracks supporting high-concurrency game state persistence.",
        url="https://careers.roblox.com/",
    ),
    InternshipResource(
        category="Alaska Game Development and Remote US Studio Database Tracks",
        organization="Hitmarker Game Industry Jobs",
        location="Remote US",
        focus="Filterable game industry board for remote backend, tools, and data engineering internships.",
        url="https://hitmarker.net/jobs",
    ),
]


def check_url(url: str) -> tuple[int, str]:
    """Verify URL using curl.exe with User-Agent and redirect following."""
    cmd = [
        "curl.exe",
        "-A",
        "Mozilla/5.0",
        "-s",
        "-L",
        "-o",
        "NUL" if sys.platform.startswith("win") else "/dev/null",
        "-w",
        "%{http_code} %{url_effective}",
        "--max-time",
        "12",
        url,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    output = proc.stdout.strip()
    if not output:
        return 0, url
    parts = output.split(" ", 1)
    try:
        code = int(parts[0])
    except ValueError:
        code = 0
    effective_url = parts[1].strip() if len(parts) > 1 else url
    return code, effective_url


def build_markdown(verified_rows: list[tuple[InternshipResource, int, str]], skipped_rows: list[tuple[InternshipResource, int, str]]) -> str:
    lines: list[str] = [
        "# Software, Database Systems, and Game Development Internships in Alaska",
        "",
        "This document catalogs verified internship portals, research institutes, Alaska employers, and remote-eligible game backend tracks suitable for an undergraduate student with strong experience in database systems.",
        "",
        "All URLs listed below were checked live via [verify_internship_links.py](verify_internship_links.py) and confirmed to return HTTP 200.",
        "",
    ]

    categories: list[str] = []
    for res, _code, _eff in verified_rows:
        if res.category not in categories:
            categories.append(res.category)

    for idx, cat in enumerate(categories, start=1):
        lines.append(f"## {idx}. {cat}")
        lines.append("")
        lines.append("| Organization | Location | Focus and Database / Software Fit | Verified URL |")
        lines.append("| :--- | :--- | :--- | :--- |")
        for res, _code, eff_url in verified_rows:
            if res.category == cat:
                lines.append(f"| **{res.organization}** | {res.location} | {res.focus} | {eff_url} |")
        lines.append("")

    lines.extend([
        "## Application Strategy for Database-Focused Undergraduates",
        "",
        "1. **Academic Credit Integration (UAA CSCE A395 / CSCE A495):** Undergraduates enrolled at UAA can pair approved local software or database internships with upper-division degree credit.",
        "2. **Recruitment Timing:** Alaska corporations, state agencies, and UAF ACEP typically open summer internship applications between December and March.",
        "3. **Portfolio Positioning for Game Backend Roles:** Highlight SQL optimization, concurrency control, distributed state storage, and telemetry data pipelines alongside engine experience.",
        "4. **Re-Verify Links Anytime:** Run `python verify_internship_links.py` from the repository root to re-check HTTP status codes across all entries.",
        "",
    ])

    if skipped_rows:
        lines.append("## Excluded Non-200 Endpoints During Last Check")
        lines.append("")
        for res, code, _eff in skipped_rows:
            lines.append(f"- `{res.organization}` (`{res.url}` returned HTTP {code})")
        lines.append("")

    return "\n".join(lines)


def main() -> int:
    verified: list[tuple[InternshipResource, int, str]] = []
    skipped: list[tuple[InternshipResource, int, str]] = []

    print(f"Checking {len(CANDIDATE_RESOURCES)} candidate URLs...")
    for res in CANDIDATE_RESOURCES:
        code, effective_url = check_url(res.url)
        status_label = "OK" if code == 200 else "SKIP"
        print(f"[{status_label}] HTTP {code} | {res.url} -> {effective_url}")
        if code == 200:
            verified.append((res, code, effective_url))
        else:
            skipped.append((res, code, effective_url))

    if not verified:
        print("Error: No candidate URLs resolved to HTTP 200.", file=sys.stderr)
        return 1

    output_path = Path(__file__).resolve().parent / "alaska_software_internships.md"
    output_path.write_text(build_markdown(verified, skipped), encoding="utf-8")
    print(f"\nWrote {len(verified)} verified HTTP 200 entries to {output_path} ({len(skipped)} non-200 skipped).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())