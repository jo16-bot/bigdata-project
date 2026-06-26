# RUNBOOK: NYC Taxi Data Analysis (Project 2026)

## 1. Προαπαιτούμενα
- Spark 3.x εγκατεστημένο.
- Πρόσβαση στο Kubernetes cluster (via VPN).
- KUBECONFIG σωστά ρυθμισμένο.

## 2. Οδηγίες Εκτέλεσης
Για κάθε ερώτημα (Q1-Q6), ακολουθήστε τα παρακάτω βήματα:

1. Εξαγωγή του kubeconfig: `export KUBECONFIG=~/.kube/config`
2. Μετάβαση στον φάκελο: `cd ~/bigdata-uth/project2026/`
3. Εκτέλεση submission script: `bash scripts/submit_qX.sh`

## 3. Αντιμετώπιση Προβλημάτων (Troubleshooting)
- **i/o timeout στο kubectl:** Σημαίνει απώλεια σύνδεσης με το VPN. Ελέγξτε αν είστε συνδεδεμένοι στο δίκτυο του πανεπιστημίου.
- **UnknownHostException:** Πρόβλημα DNS στο HDFS NameNode. Απαιτείται επανεκκίνηση του driver pod.
