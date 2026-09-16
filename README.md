# starling

                                                                      #  _
                                                                    _# ,#
            _                                                    _@#@##__#
            _%_                        _,,_               _ __@@######@#"
            ,@%@@,__                @@======,         _@======*######*@"
             _"@#******=@==@@@,__      @===*=_    _@@=======*######*@#"
              '"@#*######*=========@@@_@===**#@@=========***#####*@#"
                 ""@*#######***========================****###**@"
                      @##*#*#*+****==================*+*==#=*@"
                         '###*=**=**+==============+=**===#""
                              "%#@@===*@=========@@*=@@""
                                   "'"" %========
                                        _========@_
                                      _@======%=====,_
                                     @===%===%========@,
                                     "#==============@"
               __             ___      ^"##@=@=@=@#"
         _____/ /_____ ______/ (_)___  ____ _
        / ___/ __/ __ `/ ___/ / / __ \/ __ `/
       (__  ) /_/ /_/ / /  / / / / / / /_/ /
      /____/\__/\__,_/_/  /_/_/_/ /_/\__, /
                                    /____/

The relay agents talk through instead of talking to each other.

An agent posts a message; starling establishes who sent it from the pod's
Kubernetes token, checks it against a schema, drops what should not
propagate, delivers it, and counts everything on the way past.

**A sender cannot write its own name.** There is no `from` field, and a
payload carrying one is rejected rather than ignored. Identity comes from
the service-account token every pod already mounts, signed by the API
server and unobtainable by any other pod.

    go build ./...   &&   go test ./...
    bin/deploy.sh local        # into a k3d cluster named in kube/environments

Two pods talk only after one asks for a ticket, which closes after thirty
minutes idle. A lost store loses undelivered mail, by design; the remedy is
to roll the deployment.

    USING.md            running it, the endpoints, message forms, tickets
    DESIGN.md           what was considered, what was rejected, and why
    OPERATION.md        for the person who has been woken up
    proto/starling/v1/  the contract, and LICENSE.txt, how it may be used
