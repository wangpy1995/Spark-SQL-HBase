package org.apache.spark.sql.hbase

import org.apache.spark.annotation.{Experimental, InterfaceStability}
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlanner
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.{BaseSessionStateBuilder, SessionState}
import org.apache.spark.sql.{SparkSession, Strategy}

/**
  * Created by wpy on 17-5-17.
  */
@Experimental
@InterfaceStability.Unstable
class HBaseSessionBuilder(session: SparkSession, parentState: Option[SessionState] = None)
  extends BaseSessionStateBuilder(session, parentState) {

  def this(hbaseSession: HBaseSession) {
    this(hbaseSession.asInstanceOf[SparkSession], None)
  }

  private def externalCatalog: HBaseExternalCatalog =
    session.sharedState.externalCatalog.asInstanceOf[HBaseExternalCatalog]


  /**
    * A logical query plan `Analyzer` with rules specific to Hive.
    */
  override protected def analyzer: Analyzer = new Analyzer(catalog, conf) {
    override val extendedResolutionRules: Seq[Rule[LogicalPlan]] =
      new FindDataSourceTable(session) +:
        new ResolveSQLOnFile(session) +:
        customResolutionRules

    override val postHocResolutionRules: Seq[Rule[LogicalPlan]] =
      PreprocessTableCreation(session) +:
        PreprocessTableInsertion(conf) +:
        DataSourceAnalysis(conf) +:
        HBaseAnalysis +:
        customPostHocResolutionRules

    override val extendedCheckRules: Seq[LogicalPlan => Unit] =
      PreWriteCheck +:
        customCheckRules
  }

  override protected def planner: SparkPlanner = {
    new SparkPlanner(session.sparkContext, conf, experimentalMethods) with HBaseStrategies {
      override val sparkSession: SparkSession = session

      override def extraPlanningStrategies: Seq[Strategy] =
        super.extraPlanningStrategies ++ customPlanningStrategies

      override def strategies: Seq[Strategy] = {
        experimentalMethods.extraStrategies ++
          extraPlanningStrategies ++ Seq(
          HBaseTableScans,
          HBaseDataSource,
          FileSourceStrategy,
          DataSourceStrategy(conf),
          SpecialLimits,
          InMemoryScans,
          Aggregation,
          JoinSelection,
          BasicOperators
        )
      }
    }
  }

  override protected lazy val catalog: HBaseSessionCatalog = {
    val catalog = new HBaseSessionCatalog(
      externalCatalog,
      session.sharedState.globalTempViewManager,
      functionRegistry,
      conf,
      SessionState.newHadoopConf(session.sparkContext.hadoopConfiguration, conf),
      sqlParser,
      resourceLoader)
    parentState.foreach(_.catalog.copyStateTo(catalog))
    catalog
  }

  override protected def newBuilder: NewBuilder = new HBaseSessionBuilder(_, _)
}
