# 📘 Guide Pédagogique : Surveillance IoT & Détection d'Anomalies en Temps Réel

Ce guide est conçu pour vous accompagner dans la maîtrise complète de l'architecture, du code et des concepts théoriques du projet. Chaque section décortique une étape clé, propose une analogie intuitive, analyse les blocs de code critiques, présente les questions typiques posées en entretien d'embauche avec leurs réponses idéales, et suggère un mini-exercice pratique.

---

## Sommaire
1. [Étape 1 : Infrastructure & Orchestration Docker](#étape-1--infrastructure--orchestration-docker)
2. [Étape 2 : Simulateur IoT & Injection d'Anomalies](#étape-2--simulateur-iot--injection-danomalies)
3. [Étape 3 : Ingestion & Traitement de Flux (MQTT & InfluxDB)](#étape-3--ingestion--traitement-de-flux-mqtt--influxdb)
4. [Étape 4 : Détection d'Anomalies (Z-Score & Isolation Forest)](#étape-4--détection-danomalies-z-score--isolation-forest)
5. [Étape 5 : Provisioning & Tableaux de Bord Grafana](#étape-5--provisioning--tableaux-de-bord-grafana)
6. [Étape 6 : Tests Automatisés & Intégration Continue (CI)](#étape-6--tests-automatisés--intégration-continue-ci)

---

## Étape 1 : Infrastructure & Orchestration Docker

### 1. Objectif & Place dans l'Architecture
* **Place** : Socle d'exécution unifié.
* **Analogie** : L'infrastructure Docker Compose est comme une usine modulaire préfabriquée : chaque conteneur est un atelier spécialisé (le standardiste téléphonique Mosquitto, l'archiviste InfluxDB, l'écran de contrôle Grafana). Plutôt que de bâtir chaque pièce à la main sur votre machine, un seul plan (`docker-compose.yml`) assemble et connecte tous les ateliers instantanément.

### 2. Explication des Blocs Clés
* **Isolation réseau (`networks: iot_network`)** : Tous les conteneurs partagent un réseau virtuel bridge dédié. Ils communiquent par leurs noms d'hôtes DNS internes (`mosquitto`, `influxdb`, `grafana`), sans exposer inutilement leurs ports sur l'extérieur.
* **Volumes nommés (`mosquitto_data`, `influxdb_data`, `grafana_data`)** : Les conteneurs sont éphémères par nature. Si un conteneur redémarre ou est mis à jour, les volumes garantissent la persistance des métriques et des configurations.
* **Healthchecks et dépendances conditionnelles (`condition: service_healthy`)** :
  ```yaml
  depends_on:
    mosquitto:
      condition: service_healthy
    influxdb:
      condition: service_healthy
  ```
  *Pourquoi ?* Un simple `depends_on` classique vérifie seulement que le conteneur a été démarré, pas que le service applicatif est prêt à recevoir des connexions. Le healthcheck (`influx ping` ou `mosquitto_sub`) évite que le script Python d'ingestion ne crashe en tentant de se connecter à une base de données encore en cours d'initialisation.
* **Sécurité des configurations (`.env.example` vs `.env`)** : Aucun mot de passe ni token n'est inscrit en dur dans le code ou le fichier compose. Le fichier `.gitignore` bloque strictement `.env`, évitant toute fuite d'identifiants sur GitHub.

### 3. Concepts Clés à Maîtriser
* **Conteneur vs Image** : Une image est un modèle figé en lecture seule ; un conteneur est une instance vivante et isolée de cette image.
* **Healthcheck Docker** : Commande interne périodique exécutée par le démon Docker pour valider l'état fonctionnel réel du service.

### 4. Questions d'Entretien
> **Q1 : Pourquoi utiliser `condition: service_healthy` plutôt qu'un `restart: always` ou un simple `depends_on` ?**
> * **Réponse modèle** : Un `depends_on` basique ordonne uniquement le démarrage des conteneurs. Si InfluxDB met 5 secondes à créer son schéma interne, l'ingesteur Python démarré immédiatement échouera avec une erreur de connexion réseau. `service_healthy` bloque le démarrage des services dépendants jusqu'à ce que la sonde (`influx ping`) valide que la base est opérationnelle.

> **Q2 : Pourquoi avoir externalisé les données dans des volumes Docker ?**
> * **Réponse modèle** : Les conteneurs respectent le principe d'immuabilité (stateless). Toute donnée écrite dans la couche d'écriture du conteneur est détruite à sa suppression (`docker compose down`). Les volumes déportent le stockage sur l'hôte, garantissant la persistance des séries temporelles.

### 5. Mini-Exercice
* Ouvrez `.env` et modifiez le port externe de Grafana de `3000` à `3001` (`GRAFANA_PORT=3001`). Relancez et vérifiez que Grafana répond sur `http://localhost:3001`.

---

## Étape 2 : Simulateur IoT & Injection d'Anomalies

### 1. Objectif & Place dans l'Architecture
* **Place** : Source émettrice de télémétrie en amont.
* **Analogie** : Le simulateur joue le rôle d'un banc d'essai d'usine où tournent de vrais moteurs industriels équipés de capteurs physiques de température, vibration, pression et humidité.

### 2. Explication des Blocs Clés
* **Génération Gaussienne (`random.gauss(mean, std)`)** :
  Les capteurs réels ne retournent jamais une valeur rigoureusement constante. Ils sont soumis à un bruit thermique et électronique modélisé par une loi normale $\mathcal{N}(\mu, \sigma^2)$.
* **Injection contrôlée d'anomalies (`ANOMALY_RATE`)** :
  Le simulateur intègre trois signatures d'anomalies industrielles réelles :
  1. *Pic instantané (Spike)* : Choc mécanique ou surtension ponctuelle ($\pm k \cdot \sigma$).
  2. *Dérive progressive (Drift)* : Usure progressive d'un roulement ou encrassement thermique sur plusieurs cycles successifs.
  3. *Capteur bloqué (Stuck/Flatline)* : Panne du transducteur qui fige sa valeur sans aucun bruit blanc.
* **Structure hiérarchique des Topics MQTT** :
  Format : `factory/<machine_id>/<sensor_id>` (ex. `factory/machine_1/vibration`).
  *Pourquoi ?* Cette hiérarchie permet aux abonnés de filtrer très précisément les messages avec des wildcards (`factory/+/temperature` pour toutes les températures, ou `factory/machine_1/#` pour toute la machine 1).

### 3. Concepts Clés à Maîtriser
* **MQTT (Message Queuing Telemetry Transport)** : Protocole de messagerie léger basé sur le patron Publication / Souscription (Pub/Sub), conçu pour les réseaux contraints et l'IoT.
* **Niveaux de QoS MQTT** :
  - QoS 0 : *At most once* (au mieux, sans garantie).
  - QoS 1 : *At least once* (au moins une fois, avec accusé de réception PUBACK) - **notre choix ici pour éviter la perte de données critiques**.
  - QoS 2 : *Exactly once* (exactement une fois, avec négociation en 4 étapes).

### 4. Questions d'Entretien
> **Q1 : Pourquoi avoir choisi MQTT plutôt qu'une API REST HTTP pour envoyer la télémétrie des capteurs ?**
> * **Réponse modèle** : HTTP est un protocole synchrone client-serveur avec un en-tête volumineux (plusieurs centaines d'octets de headers pour chaque requête), ce qui entraîne une surcharge réseau inadaptée à des capteurs transmettant chaque seconde. MQTT est un protocole binaire bidirectionnel asynchrone ultra-léger (en-tête de 2 octets), supportant le broadcast via broker et le découplage temporel total entre émetteurs et récepteurs.

> **Q2 : Pourquoi avoir modélisé plusieurs types d'anomalies (spike, drift, stuck) plutôt qu'uniquement des valeurs extrêmes ?**
> * **Réponse modèle** : Dans l'industrie, une valeur anormale n'est pas toujours un pic géant. Un capteur bloqué à 50°C pile sans la moindre fluctuation gaussienne est souvent le signe d'un gel mémoire ou d'un fil déconnecté. Une dérive lente traduit une dégradation prédictive avant la panne critique.

### 5. Mini-Exercice
* Dans `simulator/simulator.py`, ajoutez un 5e capteur : `"power"` (puissance électrique en kW, moyenne 12.0 kW, écart-type 0.8, unité `"kW"`).

---

## Étape 3 : Ingestion & Traitement de Flux (MQTT & InfluxDB)

### 1. Objectif & Place dans l'Architecture
* **Place** : Hub de traitement intermédiaire entre le transport messager et la persistance analytique.
* **Analogie** : L'ingesteur agit comme l'inspecteur qualité à l'entrée d'un entrepôt : il déballe chaque colis MQTT, vérifie sa conformité, inspecte si le contenu est régulier, appose un tampon d'anomalie si nécessaire, et range la donnée sur l'étagère exacte InfluxDB.

### 2. Explication des Blocs Clés
* **Souscription Wildcard (`factory/#`)** :
  Le symbole `#` est le wildcard multi-niveaux de MQTT. Il permet à l'ingesteur d'écouter automatiquement toutes les machines et tous les capteurs existants et futurs sans modifier une ligne de code.
* **Validation défensive du schéma (`validate_payload`)** :
  ```python
  val = data["value"]
  if isinstance(val, bool) or not isinstance(val, (int, float)):
      return False
  ```
  *Attention piège en Python* : `bool` hérite de `int` (`isinstance(True, int)` renvoie `True`). Rejeter explicitement les booléens garantit qu'une mauvaise charge utile JSON ne corrompt pas le type de champ dans InfluxDB.
* **L'écriture InfluxDB 2.x (Line Protocol)** :
  Chaque point est modélisé ainsi :
  - **Measurement** : `sensor_telemetry` (l'équivalent de la table).
  - **Tags (indexés)** : `machine_id`, `sensor_id`, `detector` (permet des filtrages ultra-rapides).
  - **Fields (non indexés, typés)** : `value` (float), `is_anomaly` (entier 0 ou 1), `anomaly_score` (float).
  - **Timestamp** : Horodatage précis à la milliseconde en UTC.

### 3. Concepts Clés à Maîtriser
* **Tags vs Fields dans InfluxDB** :
  - *Tags* : Chaînes de caractères indexées dans les séries temporelles de la base. Réservés aux métadonnées à faible ou moyenne cardinalité (`machine_id`, `sensor_id`).
  - *Fields* : Valeurs numériques ou textuelles réelles non indexées. La mesure elle-même. Si vous mettez la valeur numérique du capteur dans un tag, vous faites exploser la cardinalité (*index explosion*) et saturez la RAM !

### 4. Questions d'Entretien
> **Q1 : Quelle est la différence fondamentale entre une base relationnelle (PostgreSQL) et une base de séries temporelles (InfluxDB) ?**
> * **Réponse modèle** : Les bases relationnelles organisent les données en tables avec contraintes d'intégrité et transactions ACID coûteuses. InfluxDB est un moteur Time-Series (TSM - Time Structured Merge Tree) optimisé pour des écritures massives en continu, un compactage automatique par fenêtres temporelles, et des politiques de rétention automatiques (*retention policies*) supprimant les anciennes données sans verrouiller la base.

> **Q2 : Pourquoi stocker `is_anomaly` sous la forme d'un entier `0/1` plutôt qu'un booléen `True/False` ?**
> * **Réponse modèle** : Dans les moteurs de requêtes analytiques (Flux InfluxDB ou PromQL), un entier permet d'appliquer directement des fonctions d'agrégation mathématique comme `sum()` pour calculer en un seul appel le nombre total d'anomalies sur les 15 dernières minutes.

### 5. Mini-Exercice
* Modifiez `ingestor/ingestor.py` pour ajouter un tag supplémentaire `unit` dans le point InfluxDB (`.tag("unit", unit)` au lieu d'un field). Observez le résultat dans InfluxDB.

---

## Étape 4 : Détection d'Anomalies (Z-Score & Isolation Forest)

### 1. Objectif & Place dans l'Architecture
* **Place** : Moteur décisionnel de l'ingesteur.
* **Analogie** : Le Z-score est comme un agent de sécurité avec une toise mesurant l'écart à la moyenne de la foule : toute personne mesurant 3 mètres attire immédiatement l'attention. L'Isolation Forest est comme un algorithme de tri qui cherche à isoler un individu en lui posant des questions binaires aléatoires : un individu excentrique se retrouve isolé en très peu de questions.

### 2. Explication des Blocs Clés
* **Interface commune `BaseDetector`** :
  Permet d'interchanger le détecteur par simple variable d'environnement (`INGESTOR_DETECTOR_TYPE=zscore` ou `isolation_forest`) selon le patron de conception **Stratégie (Strategy Pattern)**.
* **Algorithme 1 : Z-Score glissant (`ZScoreDetector`)** :
  $$z = \frac{|x - \mu|}{\sigma}$$
  - Utilise un tampon `deque(maxlen=window_size)` en $\mathcal{O}(1)$ mémoire et temps.
  - Gestion du cas limite où l'écart-type $\sigma = 0$ (signal plat) pour éviter une division par zéro.
  - Déclenche si $z > \text{threshold}$ (seuil typique : 3, soit 99.7% de confiance selon la règle empirique de la loi normale).
* **Algorithme 2 : Isolation Forest (`IsolationForestDetector`)** :
  - Algorithme d'apprentissage non supervisé issu de Scikit-Learn.
  - Isole les anomalies par partitionnement récursif de l'espace avec des arbres de décision aléatoires.
  - Phase d'échauffement (*warmup*) : collecte $N$ mesures normales avant d'ajuster (`fit`) le modèle.
  - `decision_function(x)` renvoie un score négatif pour les anomalies, normalisé par notre classe.
* **Gestion par flux distinct (`(machine_id, sensor_id)`)** :
  Chaque capteur a sa propre moyenne et sa propre variance. L'ingesteur instancie un détecteur dédié par couple machine/capteur dans un dictionnaire `self.detectors`.

### 3. Concepts Clés à Maîtriser
* **Approche Statistique vs Machine Learning** :
  - *Z-Score* : Paramétrique, suppose une distribution approximativement gaussienne, ultra-rapide, zéro apprentissage préalable.
  - *Isolation Forest* : Non paramétrique, s'adapte à des distributions asymétriques ou multimodales complexes.

### 4. Questions d'Entretien
> **Q1 : Que se passe-t-il si la fenêtre glissante du Z-score intègre l'anomalie ?**
> * **Réponse modèle** : C'est le phénomène de pollution de la fenêtre (*masking effect*). Si une anomalie persiste ou si elle est ajoutée à la fenêtre, la moyenne $\mu$ se décale et l'écart-type $\sigma$ augmente fortement, ce qui diminue le Z-score des anomalies suivantes. Une variante robuste consiste à n'ajouter dans la fenêtre que les points déclarés sains, ou à utiliser la médiane et le MAD (*Median Absolute Deviation*).

> **Q2 : Pourquoi l'Isolation Forest est-il particulièrement efficace pour détecter des anomalies par rapport à un SVM mono-classe ou un Auto-encodeur ?**
> * **Réponse modèle** : La plupart des algorithmes tentent d'apprendre la zone normale (très coûteux en calcul). L'Isolation Forest exploite le fait que les anomalies sont "peu nombreuses et différentes" : elles ont donc une longueur de chemin moyenne (*average path length*) beaucoup plus courte dans des arbres binaires aléatoires, rendant l'inférence extrêmement rapide et économe en mémoire.

### 5. Mini-Exercice
* Dans `.env`, passez `ZSCORE_THRESHOLD=2.0` (au lieu de 3.0). Observez comment le nombre d'anomalies détectées augmente sur Grafana (sensibilité accrue aux fluctuations mineures).

---

## Étape 5 : Provisioning & Tableaux de Bord Grafana

### 1. Objectif & Place dans l'Architecture
* **Place** : Couche de visualisation et d'alerte en temps réel pour les opérateurs de l'usine.
* **Analogie** : Le tableau de bord du cockpit d'un avion : les pilotes ne lisent pas des lignes de code SQL, ils surveillent des jauges colorées et des courbes avec des signaux d'alerte lumineux rouges.

### 2. Explication des Blocs Clés
* **Provisioning déclaratif (`grafana/provisioning/`)** :
  - `datasources/influxdb.yml` : Connecte Grafana à InfluxDB 2.x via le langage Flux avec token d'authentification sécurisé.
  - `dashboards/dashboards.yml` : Indique à Grafana de charger les fichiers JSON du dossier sans intervention manuelle dans l'interface web.
  - *Avantage de production* : Le tableau de bord est versionné dans Git (*Dashboard-as-Code*). Tout déploiement recrée l'interface à l'identique.
* **Requêtes Flux InfluxDB 2.x** :
  ```flux
  from(bucket: "sensor_telemetry")
    |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
    |> filter(fn: (r) => r["_measurement"] == "sensor_telemetry")
    |> filter(fn: (r) => r["sensor_id"] == "temperature")
    |> filter(fn: (r) => r["_field"] == "value")
    |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)
  ```
* **Surbrillance des Anomalies** :
  Une seconde série superposée filtre `r["_field"] == "is_anomaly"` et `r["_value"] == 1`, affichée sous forme de points rouges distincts sur la courbe temporelle.

### 3. Concepts Clés à Maîtriser
* **Flux vs InfluxQL** : Flux est un langage de script fonctionnel puissant introduit dans InfluxDB 2.x permettant des jointures, des calculs de dérive et des pipelines de transformation de séries temporelles avancés.
* **Dashboard-as-Code** : Pratique DevOps consistant à définir les tableaux de bord sous forme de code déclaratif JSON/YAML, permettant les revues de code (PR) et la reproductibilité des environnements.

### 4. Questions d'Entretien
> **Q1 : Comment Grafana sait-il se connecter à InfluxDB dès le premier lancement sans configuration manuelle ?**
> * **Réponse modèle** : Grâce au mécanisme de provisioning de Grafana. Au démarrage, le conteneur scanne le répertoire `/etc/grafana/provisioning/datasources/` et applique automatiquement la configuration YAML qui spécifie l'URL proxy interne `http://influxdb:8086`, le bucket par défaut et le token d'authentification.

### 5. Mini-Exercice
* Dans `grafana/provisioning/dashboards/iot_anomaly_dashboard.json`, modifiez la fréquence de rafraîchissement par défaut de `"refresh": "5s"` à `"refresh": "2s"`.

---

## Étape 6 : Tests Automatisés & Intégration Continue (CI)

### 1. Objectif & Place dans l'Architecture
* **Place** : Filet de sécurité garantissant la non-régression et la qualité du code avant mise en production.
* **Analogie** : Le banc de crash-test et de contrôle technique d'une voiture avant sa sortie d'usine. Aucun véhicule ne prend la route sans avoir validé 100% des tests.

### 2. Explication des Blocs Clés
* **Isolation des tests unitaires (`tests/conftest.py`)** :
  Injecte dynamiquement les dossiers `ingestor/` et `simulator/` dans le `sys.path` pour tester la logique métier sans dépendre de l'installation des paquets en mode système.
* **Tests de non-régression et cas limites (`test_detectors.py`)** :
  - Vérification du comportement sur bruit blanc gaussien (0 fausse alerte).
  - Détection garantie sur pics positifs et négatifs.
  - Résistance au cas pathologique de variance nulle ($\sigma = 0$).
* **Pipeline GitHub Actions (`.github/workflows/ci.yml`)** :
  - Déclenché à chaque `push` ou `pull_request`.
  - Étape 1 : Analyse statique avec **Ruff** (linter ultra-rapide vérifiant le respect des standards PEP 8 et la propreté des imports).
  - Étape 2 : Exécution de la suite **Pytest**.
  - Étape 3 : Construction des images Docker avec Buildx pour prouver que les `Dockerfile` compilent sans accroc.

### 3. Concepts Clés à Maîtriser
* **Mocking (`unittest.mock.MagicMock`)** : Technique permettant de simuler un composant externe (ex. l'API d'écriture InfluxDB) pour tester la logique interne d'une méthode de manière hermétique sans base de données réelle.
* **Shift Left Testing** : Pratique consistant à tester la qualité et la sécurité au plus tôt dans le cycle de développement (via pre-commit, ruff et CI) plutôt qu'après le déploiement.

### 4. Questions d'Entretien
> **Q1 : Pourquoi tester le cas $\sigma = 0$ dans un détecteur Z-Score ?**
> * **Réponse modèle** : Dans un environnement industriel, un capteur en panne peut répéter indéfiniment la même valeur entière. L'écart-type de l'échantillon devient alors strictement nul. Sans gestion explicite de cette condition, le calcul du Z-score provoque une exception fatale `ZeroDivisionError`, ce qui arrêterait immédiatement le service de streaming en production.

---

## Synthèse pour vos Entretiens Techniques

Lorsque vous présentez ce projet lors d'un entretien :
1. **Accroche (30s)** : *"J'ai conçu une plateforme de surveillance IoT industrielle temps réel capable de capter les métriques de machines, de détecter des anomalies en continu via Z-score et Isolation Forest, et de les historiser dans InfluxDB avec visualisation Grafana, le tout orchestré par Docker."*
2. **Choix forts (1min)** :
   - Découplage complet via **MQTT** (protocole asynchrone léger).
   - Détection d'anomalies **isolée par flux** machine/capteur (pour respecter les baselines physiques distinctes).
   - **Dashboard-as-Code** et pipeline **CI** automatisé garantissant la reproductibilité.
3. **Résultats mesurables** : 28 tests unitaires automatisés validés, images Docker optimisées, architecture résiliente aux pannes réseau avec reconnexion automatique.
