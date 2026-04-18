"""
augmentation.py
---------------
Génère des données synthétiques pour 6 projets DevOps historiques/actifs
et les fusionne avec les données originales dans data/augmented/.

Projets embarqués dans ce script (sans projects.csv) :
  DO   | Digital Onboarding   (2020, completed)
  APG  | API Gateway          (2021, completed)
  PG   | Payment Gateway      (2022, completed)
  FDAI | Fraud Detection AI   (2023, completed)
  NEOB | Neo Bank App         (2024, active)
  CMIG | Cloud Migration      (2025, active)

Les fichiers data/raw/ ne sont JAMAIS modifiés.
Les fichiers augmentés sont créés dans data/augmented/ (*_augmented.csv).
Utilisé uniquement pour le ML (enrichissement du dataset).
"""

import csv, hashlib, random
from datetime import datetime, timedelta
from itertools import product
from pathlib import Path

random.seed(7)

ROOT_DIR = Path(__file__).resolve().parent.parent
BASE     = ROOT_DIR / "data" / "raw"
OUT_DIR  = ROOT_DIR / "data" / "augmented"

# ══════════════════════════════════════════════════════════════════════════════
# 1. POOLS DE PERSONNES  (uniquement les IDs existants dans le dataset)
# ══════════════════════════════════════════════════════════════════════════════
JIRA_PERSONS = [
    ("Ahlem Tlili",           "tlili.ahlem@attijaribank.com.tn",           "ACC57B0DEB8"),
    ("Melki Iheb",            "melki.iheb@attijaribank.com.tn",            "ACC4BBD7E47"),
    ("Fida Rebai",            "rebai.fida@attijaribank.com.tn",            "ACCB382DA97"),
    ("sarra hamzaoui",        "hamzaoui.sarra@attijaribank.com.tn",        "ACC2FD69589"),
    ("Rania Aroua",           "aroua.rania@attijaribank.com.tn",           "ACCF60866E4"),
    ("ELABED.FIRAS",          "elabed.firas@attijaribank.com.tn",          "ACC501242AA"),
    ("Ahmed Jallouli",        "jallouli.ahmed@attijaribank.com.tn",        "ACCF4BCFDB2"),
    ("Adam Chaabani",         "chaabani.adam@attijaribank.com.tn",         "ACC4280ABCF"),
    ("Aymen Barhoumi",        "barhoumi.aymen@attijaribank.com.tn",        "ACC90AE945D"),
    ("Malek Rabhi",           "rabhi.malek@attijaribank.com.tn",           "ACC1DDA3D95"),
    ("Naim Ben Assila",       "assila.naim.ben@attijaribank.com.tn",       "ACC7E9C28E4"),
    ("Skander Amdouni",       "amdouni.skander@attijaribank.com.tn",       "ACC15DC4D8A"),
    ("GHARBI.YOUNES",         "gharbi.younes@attijaribank.com.tn",         "ACC2494A2A6"),
    ("Firas Brinsi",          "brinsi.firas@attijaribank.com.tn",          "ACC1D4FE207"),
    ("Jemaa Med Amir",        "jemaa.med.amir@attijaribank.com.tn",        "ACC2E5D0D1C"),
    ("AMMAR.MARIEM",          "ammar.mariem@attijaribank.com.tn",          "ACC305D0DE7"),
    ("Ayoub Mhadheb Chammam", "chammam.ayoub.mhadheb@attijaribank.com.tn","ACC0B58DB37"),
    ("NOUR.TABOUBI",          "taboubi.nour@attijaribank.com.tn",          "ACCEBF3B5C1"),
    ("TERRAS AHMED KHALIL",   "terras.ahmed.khalil@attijaribank.com.tn",   "ACC36DE3825"),
    ("azizridane",            "azizridane@attijaribank.com.tn",            "ACC96463414"),
]

GIT_AUTHORS = [
    ("93219ff0", "bensalem.saber@gtiinfo.com.tn"),
    ("16b8dc73", "bensalem.saber@gtiinfo.com.tn"),
    ("095db500", "teber.nour@gtiinfo.com.tn"),
    ("1060bbfa", "daghfousoumaima@gtiinfo.com.tn"),
    ("0106c648", "jenhani.chaima@gtiinfo.com.tn"),
    ("75f04902", "chaouch.ahmed@attijaribank.com.tn"),
    ("a94a3fd1", "hajkacem.mouadh@gtiinfo.com.tn"),
    ("44b5d939", "hajkacem.mouadh@gtiinfo.com.tn"),
    ("2521bef3", "baccouche.yassine@gtiinfo.com.tn"),
    ("374ae545", "benmakhlouf.faouzi@gtiinfo.com.tn"),
    ("947a69f2", "benmakhlouf.faouzi@gtiinfo.com.tn"),
    ("3d8a229f", "sghari.oumayma@gtiinfo.com.tn"),
    ("5a83eabc", "ochi.salim@gtiinfo.com.tn"),
    ("c7650c51", "ochi.salim@gtiinfo.com.tn"),
    ("047ac22a", "khouja.jihen@gtiinfo.com.tn"),
    ("5f76374b", "khouja.jihene@gtiinfo.com.tn"),
    ("1846b166", "khalifa.jawher@gtiinfo.com.tn"),
    ("2acfbd7a", "daghfous.oumaima@gtiinfo.com.tn"),
    ("73c57167", "benhajsalah.yousef@attijaribank.com.tn"),
    ("b4a16b14", "mannai.mohamed@attijaribank.com.tn"),
    ("90bedccd", "bakhouch.iyed@attijaribank.com.tn"),
    ("4e718012", "devops-team@attijaribank.com.tn"),
    ("634938b8", "devops-admin@attijaribank.com.tn"),
    ("a3068886", "dridi.chaima@gtiinfo.com.tn"),
    ("a6429d6a", "jenhani.chaima@gtiinfo.com.tn"),
    ("581da307", "benyahia.firas@attijaribank.com.tn"),
]

# ══════════════════════════════════════════════════════════════════════════════
# 2. DÉFINITION DES PROJETS
# ══════════════════════════════════════════════════════════════════════════════
PROJECTS = [
    dict(key="DO",   full="Digital Onboarding", mode="sprint",
         start="2020-05-01", end="2020-11-20", status="completed",
         n_tickets=75,  sprint_prefix="DO Sprint",   sprint_len=14,
         persons=[0,1,6,7,8,9,10,14], git_authors=[0,2,5,6,10,12]),

    dict(key="APG",  full="API Gateway",         mode="deadline",
         start="2021-04-01", end="2021-12-10", status="completed",
         n_tickets=100, sprint_prefix="APG Sprint",  sprint_len=14,
         persons=[2,3,10,11,12,13,15,16], git_authors=[1,3,7,8,13,16]),

    dict(key="PG",   full="Payment Gateway",     mode="sprint",
         start="2022-01-01", end="2022-07-28", status="completed",
         n_tickets=80,  sprint_prefix="PG Sprint",   sprint_len=14,
         persons=[4,5,14,15,16,17,18,19], git_authors=[4,9,11,15,17,20]),

    dict(key="FDAI", full="Fraud Detection AI",  mode="sprint",
         start="2023-02-01", end="2023-10-05", status="completed",
         n_tickets=90,  sprint_prefix="FDAI Sprint", sprint_len=14,
         persons=[0,6,7,10,18,19,1,4], git_authors=[5,6,12,14,18,21]),

    dict(key="NEOB", full="Neo Bank App",        mode="sprint",
         start="2024-03-01", end="2026-04-15", status="active",
         n_tickets=175, sprint_prefix="NEOB Sprint", sprint_len=14,
         persons=[1,2,8,11,13,14,3,7], git_authors=[0,2,4,7,19,22]),

    dict(key="CMIG", full="Cloud Migration",     mode="deadline",
         start="2025-01-01", end="2026-04-15", status="active",
         n_tickets=120, sprint_prefix="CMIG Sprint", sprint_len=14,
         persons=[3,5,9,12,15,16,0,6], git_authors=[1,3,8,13,23,24]),
]

# ══════════════════════════════════════════════════════════════════════════════
# 3. GÉNÉRATEUR DE RÉSUMÉS DE TICKETS
# ══════════════════════════════════════════════════════════════════════════════
EPIC_DOMAINS = {
    "DO":   ["Digital KYC", "Customer Onboarding", "Identity Verification",
             "Document Management", "Account Activation", "Risk Assessment",
             "Notification Services", "Compliance & Regulatory"],
    "APG":  ["API Security", "Request Routing", "Rate Limiting & Throttling",
             "API Versioning", "Developer Portal", "Analytics & Monitoring",
             "Webhook Management", "Gateway Resilience"],
    "PG":   ["Payment Processing", "Card Integration", "Refund Management",
             "Reconciliation", "Multi-Currency", "Merchant Integration",
             "Fraud Pre-screening", "Payment Analytics"],
    "FDAI": ["Data Engineering", "ML Model Development", "Real-time Scoring",
             "Feature Engineering", "Model Monitoring", "Case Management",
             "Alert Management", "Regulatory Reporting"],
    "NEOB": ["Mobile Authentication", "Digital Wallet", "Personal Finance",
             "Card Management", "Loan & Credit", "Investment Module",
             "Peer-to-peer Payments", "Open Banking", "Notifications",
             "Account Services", "UI/UX & Accessibility", "Analytics"],
    "CMIG": ["Infrastructure Assessment", "Containerization", "Kubernetes Orchestration",
             "CI/CD Migration", "Database Migration", "Network & Security",
             "Disaster Recovery", "Monitoring & Observability",
             "Cost Optimization", "Legacy Decommission"],
}

STORY_ACTIONS = [
    "Implement", "Develop", "Design", "Build", "Create", "Integrate",
    "Configure", "Optimize", "Refactor", "Test", "Document", "Deploy",
    "Set up", "Define", "Analyse", "Validate", "Review", "Migrate",
    "Enhance", "Fix", "Monitor", "Enable", "Disable", "Update",
]

STORY_COMPONENTS = {
    "DO": [
        "KYC validation service", "document upload API", "OCR extraction engine",
        "identity verification module", "email OTP flow", "SMS OTP service",
        "biometric liveness check", "customer registration step", "risk score calculator",
        "onboarding status tracker", "admin monitoring dashboard", "AML compliance check",
        "address verification API", "data extraction pipeline", "ID card parser",
        "onboarding notification hook", "account creation trigger", "customer profile builder",
        "duplicate detection service", "face recognition module", "audit log handler",
        "consent management flow", "data retention policy", "onboarding API gateway",
        "partner integration connector", "customer journey analytics", "dropout detection",
        "selfie verification module", "proof-of-address validator", "onboarding funnel report",
    ],
    "APG": [
        "OAuth2 token service", "JWT validation middleware", "rate limiting engine",
        "API key rotation service", "request routing layer", "SSL termination handler",
        "circuit breaker module", "distributed tracing system", "API versioning strategy",
        "developer portal backend", "webhook delivery engine", "retry logic handler",
        "dead-letter queue", "load balancer configuration", "gateway health check",
        "IP whitelisting service", "request transformation layer", "response caching module",
        "API usage analytics", "SLA monitoring dashboard", "partner onboarding flow",
        "API documentation generator", "mock server setup", "traffic shaping policy",
        "error response standardisation", "contract testing suite", "canary deployment",
        "blue-green switch", "API deprecation workflow", "sandbox environment setup",
    ],
    "PG": [
        "card payment processor", "Visa/Mastercard integration", "3DS authentication",
        "refund workflow engine", "daily reconciliation job", "FX conversion service",
        "merchant SDK library", "recurring payment scheduler", "chargeback handler",
        "payment notification service", "PCI-DSS audit trail", "tokenisation service",
        "payment routing logic", "split payment module", "batch payment processor",
        "settlement report generator", "dispute management flow", "fraud pre-screening hook",
        "payment analytics dashboard", "retry payment service", "void transaction flow",
        "mobile payment SDK", "QR payment endpoint", "IBAN validation service",
        "currency conversion cache", "payment gateway logs", "transaction search API",
        "merchant dashboard", "fee calculation engine", "payment link generator",
    ],
    "FDAI": [
        "feature engineering pipeline", "transaction feature extractor", "ML training framework",
        "fraud scoring engine", "model drift detector", "SHAP explainer service",
        "synthetic fraud generator", "real-time inference API", "batch scoring job",
        "alert routing system", "investigation case manager", "label propagation model",
        "ensemble model builder", "threshold calibration tool", "regulatory report generator",
        "model performance dashboard", "A/B test framework", "feature store integration",
        "data quality checker", "fraud pattern analyser", "graph neural network module",
        "velocity rule engine", "geolocation anomaly detector", "device fingerprinting",
        "historical fraud explorer", "model registry service", "retraining scheduler",
        "decision audit log", "network analysis module", "customer risk profiler",
    ],
    "NEOB": [
        "biometric authentication", "Face ID integration", "fingerprint login",
        "digital wallet connector", "Apple Pay integration", "Google Pay integration",
        "push notification service", "spending categoriser", "budget tracker",
        "QR payment scanner", "QR code generator", "peer-to-peer transfer",
        "investment portfolio view", "fund transfer service", "card freeze feature",
        "spending limit control", "loan application flow", "instant loan approval",
        "dark mode UI", "accessibility layer", "WCAG compliance check",
        "open banking connector", "account aggregation", "statement download",
        "scheduled payment service", "bill payment integration", "beneficiary management",
        "card transaction history", "mini statement view", "currency converter",
        "loyalty points module", "referral reward system", "card request flow",
        "PIN change service", "card limit API", "dispute submission form",
        "customer support chat", "in-app notifications", "user preference settings",
        "biometric re-enrolment", "device management", "session timeout handler",
        "deep link routing", "app performance monitor", "crash analytics integration",
    ],
    "CMIG": [
        "infrastructure inventory tool", "workload assessment report", "Docker image builder",
        "Kubernetes manifest templates", "Helm chart definitions", "CI pipeline migration",
        "CD pipeline setup", "Oracle to PostgreSQL migration", "Redis cache migration",
        "VPC architecture design", "security group rules", "IAM role configuration",
        "cross-region backup setup", "disaster recovery runbook", "Prometheus monitoring",
        "Grafana dashboard templates", "ELK stack deployment", "log aggregation pipeline",
        "cost tagging policy", "rightsizing analysis", "reserved instance plan",
        "legacy server decommission", "DNS migration plan", "load balancer migration",
        "SSL certificate migration", "secrets management setup", "API gateway migration",
        "service mesh configuration", "network policy enforcement", "compliance audit report",
        "penetration test scope", "cloud security baseline", "SLA baseline measurement",
    ],
}

TASK_COMPONENTS = {
    "DO":   ["KYC form UI", "document upload UI", "OTP input screen", "dashboard widget",
             "compliance report", "onboarding analytics", "API error handling", "unit tests"],
    "APG":  ["swagger docs", "postman collection", "API test suite", "gateway config file",
             "rate limit policy", "monitoring alert", "changelog entry", "load test script"],
    "PG":   ["payment form UI", "reconciliation report", "test card data", "fee schedule",
             "merchant guide", "test automation", "error code mapping", "audit query"],
    "FDAI": ["notebook prototype", "data pipeline test", "model evaluation report",
             "feature importance chart", "confusion matrix", "threshold analysis", "unit tests"],
    "NEOB": ["screen design", "UI component", "unit test", "API mock", "e2e test scenario",
             "analytics event", "deep link", "release note", "crash report fix"],
    "CMIG": ["runbook update", "Terraform module", "Ansible playbook", "network diagram",
             "cost estimate", "rollback plan", "smoke test", "post-migration report"],
}

def make_summary_pool(proj_key, n_epics, n_stories, n_tasks):
    pool = []
    domains     = list(EPIC_DOMAINS[proj_key]);     random.shuffle(domains)
    story_comps = STORY_COMPONENTS[proj_key]
    task_comps  = TASK_COMPONENTS[proj_key]

    for i in range(n_epics):
        d = domains[i % len(domains)]
        pool.append(("Epic", f"{d} Platform", f"Platform for {d.lower()}"))

    pairs = list(product(STORY_ACTIONS, story_comps)); random.shuffle(pairs)
    for i in range(n_stories):
        a, c = pairs[i % len(pairs)]
        pool.append(("Story", f"{a} {c}", f"[Story] {a} {c}"))

    tpairs = list(product(STORY_ACTIONS, task_comps)); random.shuffle(tpairs)
    for i in range(n_tasks):
        a, c = tpairs[i % len(tpairs)]
        pool.append(("Task", f"{a} {c}", f"[Task] {a} {c}"))

    random.shuffle(pool)
    return pool

# ══════════════════════════════════════════════════════════════════════════════
# 4. WORKFLOWS JIRA
# ══════════════════════════════════════════════════════════════════════════════
STATUS_CATEGORY = {
    "To Do": "In Progress", "In Analysis": "In Progress", "In Process": "In Progress",
    "In Review": "In Progress", "Correction Post Review": "In Progress",
    "Ready For Test": "In Progress", "QA In Process": "In Progress",
    "In Correction QA": "In Progress", "Blocked": "In Progress", "On Hold": "In Progress",
    "To Be Deployed": "Done", "Done": "Done",
}

WORKFLOW_TEMPLATES = {
    "not_started": [
        ["To Do"],
    ],
    "partial": [
        ["To Do", "In Analysis"],
        ["To Do", "In Analysis", "In Process"],
        ["To Do", "In Process"],
        ["To Do", "In Analysis", "On Hold"],
        ["To Do", "In Analysis", "In Process", "On Hold"],
        ["To Do", "In Analysis", "In Process", "In Review"],
        ["To Do", "In Process", "In Review"],
        ["To Do", "In Analysis", "In Process", "Blocked"],
    ],
    "completed": [
        # Court (3-4 statuts)
        ["To Do", "In Process", "To Be Deployed"],
        ["To Do", "In Analysis", "In Process", "To Be Deployed"],
        ["To Do", "In Process", "In Review", "To Be Deployed"],
        ["To Do", "In Analysis", "In Process", "To Be Deployed"],
        # Moyen (5-6 statuts)
        ["To Do", "In Analysis", "In Process", "In Review", "To Be Deployed"],
        ["To Do", "In Analysis", "In Process", "Ready For Test", "To Be Deployed"],
        ["To Do", "In Analysis", "In Process", "In Review", "Ready For Test", "To Be Deployed"],
        ["To Do", "In Analysis", "In Process", "In Review", "Correction Post Review",
         "To Be Deployed"],
        ["To Do", "In Analysis", "In Process", "QA In Process", "To Be Deployed"],
        # Long (7-9 statuts)
        ["To Do", "In Analysis", "In Process", "In Review",
         "Correction Post Review", "Ready For Test", "To Be Deployed"],
        ["To Do", "In Analysis", "In Process", "In Review",
         "Correction Post Review", "Ready For Test", "QA In Process", "To Be Deployed"],
        ["To Do", "In Analysis", "In Process", "In Review",
         "In Correction QA", "Ready For Test", "QA In Process", "To Be Deployed"],
        # Très long non-linéaire avec Blocked
        ["To Do", "In Analysis", "In Process", "Blocked", "In Process",
         "In Review", "Correction Post Review", "Ready For Test",
         "QA In Process", "To Be Deployed"],
    ],
}

COMPLETION_WEIGHTS = {"not_started": 22, "partial": 25, "completed": 53}

# ══════════════════════════════════════════════════════════════════════════════
# 5. UTILITAIRES
# ══════════════════════════════════════════════════════════════════════════════
EXPORT_TS   = "2026-04-15 22:39:04"
EXPORT_DATE = datetime(2026, 4, 15, 22, 39, 4)

def pd(s):      return datetime.strptime(s, "%Y-%m-%d")
def fmt(dt):    return dt.strftime("%Y-%m-%d %H:%M:%S")
def fmt_tz(dt): return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "000+00:00"
def fmt_iso(dt):return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
def rand_sha(): return hashlib.sha1(str(random.random()).encode()).hexdigest()
def hrs(a, b):  return abs((b - a).total_seconds() / 3600)

def get_sprint(proj, dt):
    start = pd(proj["start"]); slen = proj["sprint_len"]
    snum  = max(0, (dt - start).days) // slen + 1
    due   = min(start + timedelta(days=slen * snum), pd(proj["end"]))
    return snum, due

COMMIT_VERBS = ["implement", "add", "fix", "refactor", "update", "integrate",
                "configure", "optimise", "test", "remove", "migrate", "improve"]

ALL_JOBS = ["semgrep-sast", "secret_detection", "gemnasium-maven-dependency_scanning",
            "kics-iac-sast", "brakeman-sast", "install-dependencies", "build",
            "code_quality", "unit-tests", "integration-tests", "accounts",
            "customers", "trade", "deploy-staging", "smoke-tests"]

# ══════════════════════════════════════════════════════════════════════════════
# 6. GÉNÉRATION JIRA
# ══════════════════════════════════════════════════════════════════════════════
def build_jira_rows(proj, ticket_key, ticket_id, template, t_start, t_end, assignee):
    name, email, acct = assignee
    itype, summary, desc = template
    proj_end = pd(proj["end"])

    comp_type = random.choices(
        list(COMPLETION_WEIGHTS.keys()),
        weights=list(COMPLETION_WEIGHTS.values())
    )[0]
    workflow = random.choice(WORKFLOW_TEMPLATES[comp_type])
    snum, due = get_sprint(proj, t_start)
    sprint_str = f"{proj['sprint_prefix']} {snum}" if proj["mode"] == "sprint" else ""
    due_str    = due.strftime("%Y-%m-%d")

    rows   = []
    cur_dt = t_start + timedelta(hours=random.uniform(1, 8))

    for i, status in enumerate(workflow):
        is_last  = (i == len(workflow) - 1)
        entry_dt = cur_dt

        if not is_last:
            exit_dt = entry_dt + timedelta(hours=random.uniform(18, 110))
            if exit_dt > proj_end:
                exit_dt = proj_end - timedelta(hours=random.uniform(2, 20))
            if exit_dt <= entry_dt:
                exit_dt = entry_dt + timedelta(hours=20)
            rows.append({
                "TicketKey": ticket_key, "TicketID": ticket_id,
                "Project": proj["full"], "ProjectKey": proj["key"],
                "Summary": summary, "Description": desc,
                "IssueType": itype, "Status": status,
                "StatusCategory": STATUS_CATEGORY[status], "IsCurrent": "False",
                "StatusEntryDate": fmt(entry_dt), "StatusExitDate": fmt(exit_dt),
                "TimeInStatusHours": round(hrs(entry_dt, exit_dt), 2),
                "Priority": "Medium",
                "Assignee": name, "AssigneeEmail": email, "AssigneeAccountID": acct,
                "Created": fmt(t_start), "Updated": fmt(exit_dt),
                "ResolutionDate": "", "DueDate": due_str,
                "SprintState": sprint_str, "deadline": due_str,
            })
            cur_dt = exit_dt + timedelta(hours=random.uniform(0.5, 6))
        else:
            resolution = ""
            if proj["status"] == "completed" and status in ("To Be Deployed", "Done"):
                resolution = fmt(proj_end - timedelta(days=random.randint(1, 14)))
            rows.append({
                "TicketKey": ticket_key, "TicketID": ticket_id,
                "Project": proj["full"], "ProjectKey": proj["key"],
                "Summary": summary, "Description": desc,
                "IssueType": itype, "Status": status,
                "StatusCategory": STATUS_CATEGORY[status], "IsCurrent": "True",
                "StatusEntryDate": fmt(entry_dt), "StatusExitDate": "",
                "TimeInStatusHours": round(hrs(entry_dt, EXPORT_DATE), 2),
                "Priority": "Medium",
                "Assignee": name, "AssigneeEmail": email, "AssigneeAccountID": acct,
                "Created": fmt(t_start), "Updated": EXPORT_TS,
                "ResolutionDate": resolution, "DueDate": due_str,
                "SprintState": sprint_str, "deadline": due_str,
            })
    return rows

# ══════════════════════════════════════════════════════════════════════════════
# 7. GÉNÉRATION GIT
# ══════════════════════════════════════════════════════════════════════════════
def build_commits(proj, ticket_key, t_start, t_end, author):
    aid, aemail = author
    comps = STORY_COMPONENTS[proj["key"]]
    span  = (t_end - t_start).total_seconds()
    commits = []
    for _ in range(random.randint(3, 8)):
        cdt = t_start + timedelta(seconds=random.uniform(0.05, 0.95) * span)
        commits.append({
            "sha":    rand_sha(),
            "author": aid,
            "email":  aemail,
            "date":   cdt.strftime("%Y-%m-%d %H:%M:%S+01:00"),
            "title":  f"{ticket_key}: {random.choice(COMMIT_VERBS)} {random.choice(comps)}",
            "_dt":    cdt,
        })
    return sorted(commits, key=lambda x: x["_dt"])

def build_mr(proj, ticket_key, t_start, t_end, author, mr_id):
    aid, _ = author
    comps  = STORY_COMPONENTS[proj["key"]]
    title  = f"{ticket_key}: {random.choice(COMMIT_VERBS)} {random.choice(comps)}"
    created = t_start + timedelta(hours=random.uniform(4, 72))
    state   = random.choices(["merged", "merged", "merged", "opened", "closed"],
                              weights=[60, 15, 10, 8, 7])[0]
    if state == "merged":
        merged = created + timedelta(hours=random.uniform(0.5, 120))
        if merged > t_end:
            merged = t_end - timedelta(hours=0.5)
        return dict(mr_id=mr_id, title=title, author=aid, state=state,
                    created_at=fmt_tz(created), merged_at=fmt_tz(merged),
                    merge_time_hours=round((merged - created).total_seconds() / 3600, 6))
    return dict(mr_id=mr_id, title=title, author=aid, state=state,
                created_at=fmt_tz(created), merged_at="", merge_time_hours="")

def build_pipelines_jobs(commits, mr_id, pid, jid):
    pipelines, jobs = [], []
    for commit in commits:
        sha, cdt = commit["sha"], commit["_dt"]
        refs = ["mr", "develop"] if random.random() < 0.45 else ["mr"]
        for ref_type in refs:
            ref      = f"refs/merge-requests/{mr_id}/head" if ref_type == "mr" else "develop"
            p_status = random.choices(["success", "failed", "canceled", "success"],
                                       weights=[55, 20, 8, 17])[0]
            duration = round(random.uniform(3, 80), 6)
            p_start  = cdt + timedelta(minutes=random.uniform(1, 10))
            p_end_   = p_start + timedelta(minutes=duration)
            pipelines.append(dict(
                pipeline_id=pid, status=p_status, ref=ref, sha=sha,
                created_at=fmt_tz(p_start), updated_at=fmt_tz(p_end_),
                duration_minutes=duration,
            ))
            pool = random.sample(ALL_JOBS, random.randint(3, 6))
            jdt  = p_start + timedelta(seconds=15)
            for ji, jname in enumerate(pool):
                if p_status == "canceled":
                    j_status = "canceled"; j_start = jdt if ji == 0 else None
                elif p_status == "failed" and ji == len(pool) - 1:
                    j_status = "failed";   j_start = jdt
                elif p_status == "failed" and random.random() < 0.15:
                    j_status = random.choice(["skipped", "canceled"]); j_start = None
                else:
                    j_status = "success";  j_start = jdt
                j_end = (j_start or jdt) + timedelta(minutes=random.uniform(0.5, 12))
                jobs.append(dict(
                    pipeline_id=pid, job_id=jid, name=jname, status=j_status,
                    started_at=fmt_iso(j_start) if j_start else "",
                    finished_at=fmt_iso(j_end), sha=sha,
                ))
                jdt = j_end + timedelta(seconds=random.uniform(5, 30))
                jid += 1
            pid += 1
    return pipelines, jobs, pid, jid

# ══════════════════════════════════════════════════════════════════════════════
# 8. LECTURE DES MAX IDs EXISTANTS (pour éviter les conflits)
# ══════════════════════════════════════════════════════════════════════════════
def get_max_id(filepath, col, encoding="utf-8"):
    max_id = 0
    try:
        with open(filepath, encoding=encoding) as f:
            for row in csv.DictReader(f):
                try:
                    max_id = max(max_id, int(row[col]))
                except (ValueError, KeyError):
                    pass
    except FileNotFoundError:
        pass
    return max_id

# ══════════════════════════════════════════════════════════════════════════════
# 9. CRÉATION DES FICHIERS AUGMENTÉS  (original intact, nouveau fichier créé)
# ══════════════════════════════════════════════════════════════════════════════
def create_augmented_file(orig_path, out_path, new_rows, cols, encoding="utf-8"):
    """
    Lit orig_path (fichier original, non modifié),
    concatène new_rows, et écrit le résultat dans out_path (_augmented.csv).
    """
    with open(orig_path, encoding=encoding) as f:
        orig_rows = list(csv.DictReader(f))
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(orig_rows)
        w.writerows(new_rows)
    return len(orig_rows), len(new_rows)

# ══════════════════════════════════════════════════════════════════════════════
# 10. MAIN
# ══════════════════════════════════════════════════════════════════════════════
def main():
    print("=" * 60)
    print("  AUGMENTATION DES DONNEES DEVOPS")
    print("=" * 60)
    print(f"  Source  : {BASE}")
    print(f"  Sortie  : {OUT_DIR}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Lire les max IDs existants depuis data/raw pour partir sans conflit
    mr_id = get_max_id(BASE / "gitlab_merge_requests.csv",
                       "mr_id", "utf-8") + 1
    pid   = get_max_id(BASE / "gitlab_pipelines.csv",
                       "pipeline_id", "utf-8") + 1
    jid   = get_max_id(BASE / "gitlab_jobs.csv",
                       "job_id", "utf-8") + 1

    print(f"\n  IDs de depart : MR={mr_id}  Pipeline={pid}  Job={jid}")
    print()

    jira_rows, commit_rows, mr_rows, pipeline_rows, job_rows = [], [], [], [], []

    for proj in PROJECTS:
        p_start = pd(proj["start"])
        p_end   = pd(proj["end"])
        p_span  = (p_end - p_start).days
        ntix    = proj["n_tickets"]

        n_epics  = max(5, int(ntix * 0.10))
        n_tasks  = max(5, int(ntix * 0.25))
        n_stories = ntix - n_epics - n_tasks
        summaries = make_summary_pool(proj["key"], n_epics, n_stories, n_tasks)

        print(f"  Generating {proj['key']:6} : {ntix} tickets "
              f"({n_epics}E / {n_stories}S / {n_tasks}T) ...")

        for t_idx in range(ntix):
            ticket_key = f"{proj['key']}-{t_idx + 1}"
            ticket_id  = random.randint(100000, 999999)
            template   = summaries[t_idx % len(summaries)]

            frac      = t_idx / ntix
            offset_d  = int(frac * p_span * 0.85) + random.randint(0, 10)
            t_start   = p_start + timedelta(days=offset_d)
            t_len_d   = random.randint(10, min(40, max(10, p_span - offset_d - 5)))
            t_end     = min(t_start + timedelta(days=t_len_d), p_end - timedelta(days=1))
            if t_end <= t_start:
                t_end = t_start + timedelta(days=10)

            assignee   = JIRA_PERSONS[random.choice(proj["persons"])]
            git_author = GIT_AUTHORS[random.choice(proj["git_authors"])]

            # JIRA
            jrows = build_jira_rows(proj, ticket_key, ticket_id, template,
                                     t_start, t_end, assignee)
            jira_rows.extend(jrows)

            # GIT — pas de commit pour tickets non démarrés
            if jrows[-1]["Status"] == "To Do" and len(jrows) == 1:
                if random.random() < 0.35:
                    c = build_commits(proj, ticket_key, t_start,
                                      t_start + timedelta(days=2), git_author)
                    commit_rows.extend(c[:1])
                continue

            commits = build_commits(proj, ticket_key, t_start, t_end, git_author)
            commit_rows.extend(commits)

            mr = build_mr(proj, ticket_key, t_start, t_end, git_author, mr_id)
            mr_rows.append(mr)

            plines, jbs, pid, jid = build_pipelines_jobs(commits, mr_id, pid, jid)
            pipeline_rows.extend(plines)
            job_rows.extend(jbs)

            mr_id += 1

    # ── Création des fichiers augmentés ──────────────────────────────────────
    print()
    print(f"  {'Fichier cree':<45} {'Original':>9} {'Ajoute':>7} {'Total':>7}")
    print("  " + "-" * 70)

    FILES = [
        ("jira.csv",                 "jira_augmented.csv",
         jira_rows, "utf-8-sig",
         ["TicketKey","TicketID","Project","ProjectKey","Summary","Description",
          "IssueType","Status","StatusCategory","IsCurrent","StatusEntryDate",
          "StatusExitDate","TimeInStatusHours","Priority","Assignee","AssigneeEmail",
          "AssigneeAccountID","Created","Updated","ResolutionDate","DueDate",
          "SprintState","deadline"]),

        ("gitlab_commits.csv",       "gitlab_commits_augmented.csv",
         commit_rows, "utf-8",
         ["sha","author","email","date","title"]),

        ("gitlab_merge_requests.csv","gitlab_merge_requests_augmented.csv",
         mr_rows, "utf-8",
         ["mr_id","title","author","state","created_at","merged_at","merge_time_hours"]),

        ("gitlab_pipelines.csv",     "gitlab_pipelines_augmented.csv",
         pipeline_rows, "utf-8",
         ["pipeline_id","status","ref","sha","created_at","updated_at","duration_minutes"]),

        ("gitlab_jobs.csv",          "gitlab_jobs_augmented.csv",
         job_rows, "utf-8",
         ["pipeline_id","job_id","name","status","started_at","finished_at","sha"]),
    ]

    print()
    print(f"  {'Fichier cree':<45} {'Original':>9} {'Ajoute':>7} {'Total':>7}")
    print("  " + "-" * 70)

    total_avant = total_apres = 0
    for orig_name, out_name, new_rows, enc, cols in FILES:
        orig_path = BASE    / orig_name   # lecture depuis data/raw/
        out_path  = OUT_DIR / out_name    # ecriture dans data/augmented/
        n_orig, n_new = create_augmented_file(orig_path, out_path, new_rows, cols, enc)
        total = n_orig + n_new
        total_avant += n_orig
        total_apres += total
        print(f"  {out_name:<45} {n_orig:>9} {n_new:>7} {total:>7}")

    print("  " + "-" * 70)
    print(f"  {'TOTAL':<45} {total_avant:>9} {total_apres-total_avant:>7} {total_apres:>7}")
    print()
    print(f"  Fichiers originaux (data/raw/)       : INCHANGES")
    print(f"  Fichiers augmentes (data/augmented/) : *_augmented.csv")
    print("  Augmentation terminee avec succes.")
    print("=" * 60)

if __name__ == "__main__":
    main()
