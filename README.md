# Suro: Netflix's Data Pipeline

Suro is a data pipeline service for collecting, aggregating, and dispatching large volume of application events including log data. It has the following features:

- It is distributed and can be horizontally scaled.
- It supports streaming data flow, large number of connections, and high throughput.
- It allows dynamically dispatching events to different locations with flexible dispatching rules.
- It has a simple and flexible architecture to allow users to add additional data destinations.
- It fits well into NetflixOSS ecosystem
- It is a best-effort data pipeline with support of flexible retries and store-and-forward to minimize message loss

Learn more about Suro on the <a href="https://github.com/Netflix/suro/wiki">Suro Wiki</a> and the <a href="http://techblog.netflix.com/2013/12/announcing-suro-backbone-of-netflixs.html">Netflix TechBlog post</a> where Suro was introduced.

Build
-----

NetflixGraph is built via Gradle (www.gradle.org). To build from the command line:

    ./gradlew build

Support
-----

We will use the Google Group, Suro Users, to discuss issues: https://groups.google.com/forum/#!forum/suro-users
