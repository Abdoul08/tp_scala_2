sbt package
xcopy /Y "target\scala-2.12\tp*" "apps\"

REM la première commande génère le fichier jar de la classe principale main
REM la deuxième commande permet de copier le fichier spark jar généré vers le dossier apps
