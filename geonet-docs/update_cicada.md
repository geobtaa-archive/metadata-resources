# To update cicada

Note these are just the Git side of things. You'll need to rebuild the app while coding/testing, but that's another story.

Note: we probably shouldn't stick to upstream dev, as things break. Instead we should stick to tagged releases. Currently we're stuck on 3.0.4

## Make sure `upstream-develop` is current branch
`git checkout upstream-develop`

## Merge in core updates from GeoNetwork
`git fetch upstream`
`git pull upstream develop`

## Merge `upstream-develop` into `cicgddp-develop`
`git checkout cicgddp-develop`
`git merge upstream-develop`

## Add and commit the upstream additions
`git add .`
`git commit -m "upstream updated as of mm/dd/yy hh:mm"`

## Create and checkout a descriptively named feature branch off of `cicgddp-develop`
`git checkout -b my-awesome-feature`

## Do your thing, test it (as much as possible now!) then add and commit
`git add .`
`git commit -m "my sweet new feature is awesome"`

## merge feature branch into `cicgddp-develop`
`git checkout cicgddp-develop`
`git merge my-awesome-feature`
`git add .`
`git commit -m "merged in my shiny new feature branch"`

## push it up
`git push`

## log into dev, and pull changes
`git pull`

## Test it out!

## Back on your local, merge `cicgddp-develop` into `cicgddp-prod`
`git checkout cicgddp-prod`
`git merge cicgddp-develop`
`git add .`
`git commit -m "updated prod to include my-awesome-feature"`
`git push`

## log into prod and pull
`git pull`


# To Build GeoNetwork locally for testing
At the root of the `core-geonetwork` repo:

```
mvn clean install -DskipTests
cd web
mvn jetty:start
```

If you see `[INFO] Started Jetty Server`, then you should be able to visit `localhost:8080/geonetwork` and get to work testing!

# To install maven

## On a Mac
Use Homebrew.
`brew install maven`

If you don't have Homebrew, install Homebrew first.
`/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"`

## Windows
It's a Java app, so shouldn't be much different from others, but I've never had occasion to install on Windows, so ¯\_(ツ)_/¯ 

