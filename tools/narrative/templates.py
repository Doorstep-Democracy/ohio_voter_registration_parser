"""
Per-level narrative template registry.

Decision recap (locked in planning session 2026-05-26):
  - Label: 'templated-v1' (replaces 'fake-template-v1' / --fake)
  - Per-level templates with metric emphasis per level
  - Officeholder data missing -> render placeholder line
  - Cache key: hash(metrics + per_level_config_version + template_version)
"""

from __future__ import annotations

import hashlib
import json
from typing import Optional

from pipeline.cohort_lean import cohort_sides

TEMPLATE_VERSION = 'v2'
PER_LEVEL_CONFIG_VERSION = 'v1'

LEVEL_CONFIGS: dict[str, dict] = {
    'county': {
        'noun': 'county', 'small_n_threshold': 0,
        'render_generation': True, 'render_decade_trend': True,
        'embed_parent': False, 'lead_with_geography': False,
        'officeholder_offices': ['us_senator', 'us_representative',
                                 'state_senator', 'state_representative',
                                 'county_commissioner', 'sheriff', 'prosecutor'],
    },
    'precinct': {
        'noun': 'precinct', 'small_n_threshold': 500,
        'render_generation': False, 'render_decade_trend': False,
        'embed_parent': True, 'lead_with_geography': False,
        'officeholder_offices': ['precinct_captain_r', 'precinct_captain_d'],
    },
    'congressional_district': {
        'noun': 'congressional district', 'small_n_threshold': 0,
        'render_generation': False, 'render_decade_trend': True,
        'embed_parent': False, 'lead_with_geography': True,
        'officeholder_offices': ['us_representative'],
    },
    'state_senate_district': {
        'noun': 'Ohio Senate district', 'small_n_threshold': 0,
        'render_generation': False, 'render_decade_trend': True,
        'embed_parent': False, 'lead_with_geography': True,
        'officeholder_offices': ['state_senator'],
    },
    'state_representative_district': {
        'noun': 'Ohio House district', 'small_n_threshold': 0,
        'render_generation': False, 'render_decade_trend': True,
        'embed_parent': False, 'lead_with_geography': True,
        'officeholder_offices': ['state_representative'],
    },
    'city': {
        'noun': 'city', 'small_n_threshold': 1000,
        'render_generation': False, 'render_decade_trend': True,
        'embed_parent': True, 'lead_with_geography': False,
        'officeholder_offices': ['mayor', 'city_council', 'city_attorney'],
    },
    'village': {
        'noun': 'village', 'small_n_threshold': 300,
        'render_generation': False, 'render_decade_trend': False,
        'embed_parent': True, 'lead_with_geography': False,
        'officeholder_offices': ['mayor', 'village_council'],
    },
    'township': {
        'noun': 'township', 'small_n_threshold': 500,
        'render_generation': False, 'render_decade_trend': False,
        'embed_parent': True, 'lead_with_geography': False,
        'officeholder_offices': ['trustee', 'fiscal_officer'],
    },
    'local_school_district': {
        'noun': 'school district', 'small_n_threshold': 1000,
        'render_generation': False, 'render_decade_trend': True,
        'embed_parent': True, 'lead_with_geography': False,
        'officeholder_offices': ['school_board_member'],
    },
    'city_school_district': {
        'noun': 'city school district', 'small_n_threshold': 1000,
        'render_generation': False, 'render_decade_trend': True,
        'embed_parent': True, 'lead_with_geography': False,
        'officeholder_offices': ['school_board_member'],
    },
    'exempted_village_school_district': {
        'noun': 'exempted village school district', 'small_n_threshold': 500,
        'render_generation': False, 'render_decade_trend': False,
        'embed_parent': True, 'lead_with_geography': False,
        'officeholder_offices': ['school_board_member'],
    },
    'municipal_court_district': {
        'noun': 'municipal court district', 'small_n_threshold': 1000,
        'render_generation': False, 'render_decade_trend': True,
        'embed_parent': True, 'lead_with_geography': False,
        'officeholder_offices': ['municipal_court_judge'],
    },
    'county_court_district': {
        'noun': 'county court district', 'small_n_threshold': 1000,
        'render_generation': False, 'render_decade_trend': True,
        'embed_parent': True, 'lead_with_geography': False,
        'officeholder_offices': ['county_court_judge'],
    },
    'court_of_appeals_district': {
        'noun': 'court of appeals district', 'small_n_threshold': 0,
        'render_generation': False, 'render_decade_trend': True,
        'embed_parent': False, 'lead_with_geography': True,
        'officeholder_offices': ['appeals_court_judge'],
    },
}

LEVELS = tuple(LEVEL_CONFIGS.keys())

OFFICE_LABELS: dict[str, str] = {
    'us_senator': 'U.S. Senator',
    'us_representative': 'U.S. Representative',
    'state_senator': 'State Senator',
    'state_representative': 'State Representative',
    'county_commissioner': 'County Commissioner',
    'sheriff': 'Sheriff',
    'prosecutor': 'Prosecutor',
    'mayor': 'Mayor',
    'city_council': 'City Council',
    'city_attorney': 'City Attorney',
    'village_council': 'Village Council',
    'trustee': 'Township Trustee',
    'fiscal_officer': 'Fiscal Officer',
    'school_board_member': 'School Board',
    'municipal_court_judge': 'Municipal Court Judge',
    'county_court_judge': 'County Court Judge',
    'appeals_court_judge': 'Court of Appeals Judge',
    'precinct_captain_r': 'Republican Precinct Captain',
    'precinct_captain_d': 'Democratic Precinct Captain',
}


def build_metrics_for_level(level, party_json, decade_json=None,
                            generation_json=None, party_decade_json=None,
                            parent_county=None, geography_counties=None):
    if not party_json or 'chartConfig' not in party_json:
        return None
    cfg = LEVEL_CONFIGS.get(level)
    if cfg is None:
        raise ValueError(f'Unknown level: {level!r}')

    sides = cohort_sides(party_json['chartConfig'],
                         party_json.get('title') or f'{level} party chart')
    total = sides['total']
    if total == 0:
        return None

    r_lean = sides['r']
    d_lean = sides['d']
    unc = sides['unc']

    name = (party_json.get('jurisdiction_name')
            or party_json.get('precinct')
            or party_json.get('county') or '')

    metrics = {
        'level': level, 'name': name,
        'parent_county': parent_county or party_json.get('county'),
        'data_as_of': party_json.get('updated', ''),
        'total_voters': total,
        'party': {
            'r_lean_pct': round(r_lean / total * 100, 1),
            'd_lean_pct': round(d_lean / total * 100, 1),
            'unc_pct':    round(unc    / total * 100, 1),
            'pure_r_pct': round(sides['pure_r'] / total * 100, 1),
            'pure_d_pct': round(sides['pure_d'] / total * 100, 1),
            'net_lean':   round((d_lean - r_lean) / total * 100, 1),
        },
        'small_n': total < cfg['small_n_threshold'],
    }

    if cfg['render_generation'] and generation_json:
        glabels = generation_json['chartConfig']['labels']
        gdata = generation_json['chartConfig']['datasets'][0]['data']
        gtotal = sum(gdata) or 1
        metrics['generations'] = {
            glabels[i]: round(gdata[i] / gtotal * 100, 1)
            for i in range(len(glabels))
        }

    if cfg['render_decade_trend'] and party_decade_json:
        ds = party_decade_json['chartConfig']['datasets']
        ds_map = {d['label']: d['data'] for d in ds}
        def decade_lean(idx):
            r  = (ds_map.get('Pure R', [0]*10)[idx] +
                  ds_map.get('UNC – Lapsed R', [0]*10)[idx])
            d_ = (ds_map.get('Pure D', [0]*10)[idx] +
                  ds_map.get('UNC – Lapsed D', [0]*10)[idx])
            tot = sum(v[idx] for v in ds_map.values()) or 1
            return round((d_ - r) / tot * 100, 1)
        try:
            older   = round((decade_lean(4) + decade_lean(5)) / 2, 1)
            younger = round((decade_lean(8) + decade_lean(9)) / 2, 1)
            if abs(younger - older) >= 1.5:
                metrics['trend'] = {'older': older, 'younger': younger,
                                    'direction': 'bluer' if younger > older else 'redder'}
        except (IndexError, KeyError):
            pass

    if cfg['lead_with_geography'] and geography_counties:
        metrics['geography_counties'] = list(geography_counties)

    return metrics


def _lean_phrase(net):
    party = 'Democratic' if net > 0 else 'Republican'
    mag = abs(net)
    if   mag < 2.0:  magnitude = 'narrow'
    elif mag < 6.0:  magnitude = 'modest'
    elif mag < 12.0: magnitude = 'clear'
    else:            magnitude = 'pronounced'
    return party, magnitude


def _format_jurisdiction_subject(metrics, cfg):
    name  = metrics['name']
    noun  = cfg['noun']
    parent = metrics.get('parent_county')
    level = metrics['level']

    if ' (' in name and name.endswith('Co.)'):
        bare = name.split(' (')[0]
    else:
        bare = name
    bare_title = bare.title() if bare.isupper() else bare

    if level == 'county':
        return f'{bare_title} County'
    if cfg['embed_parent'] and parent:
        return f'{bare_title} ({noun} in {parent.title()} County)'
    if cfg['lead_with_geography']:
        gc = metrics.get('geography_counties') or []
        if len(gc) > 1:
            return (f'{noun.title()} {bare_title} '
                    f'(spans {len(gc)} counties; largest share in {gc[0]})')
    return f'{bare_title} {noun}' if bare_title and not bare_title.lower().endswith(noun) else (bare_title or noun.title())


def _build_total_sentence(metrics, cfg):
    subj = _format_jurisdiction_subject(metrics, cfg)
    total = f"{metrics['total_voters']:,}"
    unc   = metrics['party']['unc_pct']
    if metrics.get('small_n'):
        return (f"{subj} has {total} registered voters — a small sample, so the "
                f"figures below carry meaningful statistical uncertainty.")
    return (f"{subj} has {total} registered voters, with {unc}% unaffiliated or "
            f"lacking primary history.")


def _build_party_sentence(metrics):
    p = metrics['party']
    party_word, magnitude = _lean_phrase(p['net_lean'])
    return (f"Republican-leaning voters make up {p['r_lean_pct']}% of registrants and "
            f"Democratic-leaning voters account for {p['d_lean_pct']}%, a {magnitude} "
            f"{party_word} lean of {abs(p['net_lean']):.1f} percentage points.")


def _build_generation_sentence(metrics):
    g = metrics.get('generations')
    if not g:
        return None
    top = max(g, key=g.get)
    return f"{top} are the largest generational cohort at {g[top]}% of registrants."


def _build_trend_sentence(metrics):
    t = metrics.get('trend')
    if not t:
        return None
    return (f"Registration is trending {t['direction']} in younger age cohorts "
            f"(1990s–2000s births lean {t['younger']:+.1f}% D−R "
            f"vs {t['older']:+.1f}% for 1950s–60s cohorts).")


def _build_officeholder_block(officeholders, cfg):
    if not cfg.get('officeholder_offices'):
        return []
    officeholders = officeholders or {}
    lines = []
    for office_key in cfg['officeholder_offices']:
        label = OFFICE_LABELS.get(office_key, office_key)
        holders = officeholders.get(office_key)
        if not holders:
            lines.append(f"{label}: data not yet available.")
            continue
        if isinstance(holders, list):
            rendered = '; '.join(
                f"{h.get('name','?')}{' (' + h.get('party','') + ')' if h.get('party') else ''}"
                for h in holders)
        else:
            h = holders
            rendered = f"{h.get('name','?')}{' (' + h.get('party','') + ')' if h.get('party') else ''}"
        lines.append(f"{label}: {rendered}")
    return lines


def build_narrative(metrics, officeholders=None):
    cfg = LEVEL_CONFIGS[metrics['level']]
    sentences = [_build_total_sentence(metrics, cfg),
                 _build_party_sentence(metrics)]
    for fn in (_build_generation_sentence, _build_trend_sentence):
        s = fn(metrics)
        if s:
            sentences.append(s)
    prose = ' '.join(sentences)
    office_lines = _build_officeholder_block(officeholders, cfg)
    if office_lines:
        prose += '\n\nElected representation:\n  ' + '\n  '.join(office_lines)
    return prose


def metrics_hash(metrics, officeholders=None):
    payload = {
        'metrics': metrics,
        'officeholders': officeholders or {},
        'template_v': TEMPLATE_VERSION,
        'config_v': PER_LEVEL_CONFIG_VERSION,
    }
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode('utf-8')
    return hashlib.sha256(blob).hexdigest()[:16]
