# RUNBOOK: NYC Taxi Data Analysis (Project 2026)

1. Προαπαιτούμενα
Πριν ξεκινήσετε, βεβαιωθείτε ότι έχετε ολοκληρώσει τις παρακάτω ενέργειες:

VPN: Ενεργή σύνδεση με το δίκτυο του πανεπιστημίου.

Environment: source ~/bigdata-env.sh

Namespace: Βεβαιωθείτε ότι βρίσκεστε στο σωστό context:
kubectl config set-context --current --namespace=<το-namespace-σας>

Authentication: export KUBECONFIG=~/.kube/config

2. Αυτοματοποιημένη Εκτέλεση (Run All)
Για την πλήρη αναπαραγωγή όλων των αποτελεσμάτων χωρίς χειρωνακτική παρέμβαση, εκτελέστε το κεντρικό script:

Bash
chmod +x scripts/run_all.sh
./scripts/run_all.sh
Το script εκτελεί διαδοχικά τα Q1 έως Q6 με τα κατάλληλα ορίσματα (--input-base και --output-base).

3. Εκτέλεση ανά Εργασία (Individual Tasks)
Αν επιθυμείτε μεμονωμένη εκτέλεση, χρησιμοποιήστε το πρότυπο εντολής (αντικαθιστώντας το qX.py):

Bash
spark-submit qX.py \
    --input-base "hdfs:///data" \
    --output-base "hdfs:///user/<your_username>/output"

5. Παραμετροποίηση ΑΜ
Οι παράμετροι εξατομίκευσης (ωριαία παράθυρα, ημέρες) υπολογίζονται αυτόματα από το script βάσει του Αριθμού Μητρώου (ΑΜ). Δεν απαιτείται χειροκίνητη αλλαγή τιμών στον πηγαίο κώδικα.

6. Αντιμετώπιση Προβλημάτων (Troubleshooting)
I/O Timeout: Συνήθως οφείλεται σε διακοπή του VPN. Επανεκκινήστε το VPN και το terminal session.

UnknownHostException (DNS): Πρόκειται για γνωστό σφάλμα του DNS resolution στο cluster. Λύση: Διαγράψτε το αντίστοιχο driver pod (kubectl delete pod <pod-name>) και επανεκτελέστε την εργασία. Η τεκμηρίωση του σφάλματος βρίσκεται στο Κεφάλαιο 9.5 της αναφοράς σας.

Logs: Για να εντοπίσετε το Application ID κάθε εκτέλεσης, ανατρέξτε στο αρχείο metrics.json που παράγεται αυτόματα στον φάκελο εξόδου στο HDFS.
Για το Q6, προσθέστε το όρισμα της διάταξης:
--run-tag A (ή B ή C).
