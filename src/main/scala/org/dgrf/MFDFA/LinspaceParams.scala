package org.dgrf.MFDFA

case class LinspaceParams(var qLinSpaceStart: Double = -5.0,
                          var qLinSpaceEnd: Double = 5.0,
                          var qLinSpaceParitions: Int = 101,
                          var scaleMin: Double = 16,
                          var scaleMax: Double = 1024,
                          var scaleCount: Int = 19)
