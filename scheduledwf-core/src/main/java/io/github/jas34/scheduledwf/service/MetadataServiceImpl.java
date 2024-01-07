package io.github.jas34.scheduledwf.service;

import java.util.List;
import java.util.Optional;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.parser.CronParser;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.core.exception.ApplicationException;
import com.netflix.conductor.dao.MetadataDAO;

import io.github.jas34.scheduledwf.dao.ScheduledWfMetadataDAO;
import io.github.jas34.scheduledwf.metadata.ScheduleWfDef;
import org.springframework.beans.BeanUtils;

/**
 * @author Jasbir Singh
 */
public class MetadataServiceImpl implements MetadataService {

    private static final CronParser QUARTZ_CRON_PARSER =
            new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ));

    private final MetadataDAO metadataDAO;

    private final ScheduledWfMetadataDAO scheduleWorkflowMetadataDao;

    public MetadataServiceImpl(MetadataDAO metadataDAO, ScheduledWfMetadataDAO scheduleWorkflowMetadataDao) {
        this.metadataDAO = metadataDAO;
        this.scheduleWorkflowMetadataDao = scheduleWorkflowMetadataDao;
    }

    @Override
    public void registerScheduleWorkflowDef(ScheduleWfDef def) {
        Optional<WorkflowDef> workflowDef = metadataDAO.getWorkflowDef(def.getWfName(), def.getWfVersion());
        if (!workflowDef.isPresent()) {
            throw new ApplicationException(ApplicationException.Code.INVALID_INPUT,
                    "Cannot find the workflow definition with name=" + def.getWfName() + " and version="
                            + def.getWfVersion());
        }

        Optional<ScheduleWfDef> scheduleWfDef =
                scheduleWorkflowMetadataDao.getScheduledWorkflowDef(def.getWfName());
        if (scheduleWfDef.isPresent() && ScheduleWfDef.Status.RUN == scheduleWfDef.get().getStatus()) {
            throw new ApplicationException(ApplicationException.Code.INVALID_INPUT,
                    "ScheduleWfDef already running. Cannot accept register. First SHUTDOWN or DELETE scheduler.");
        }
        assertCronExpressionIsValid(def.getCronExpression());
        scheduleWorkflowMetadataDao.saveScheduleWorkflow(def);
    }

    @Override
    public void updateScheduledWorkflowDef(String name,
                                           ScheduleWfDef.Status status,
                                           ScheduleWfDef scheduleWfDef) {

        Optional<ScheduleWfDef> scheduledWorkflowDef =
                scheduleWorkflowMetadataDao.getScheduledWorkflowDef(name);
        if (!scheduledWorkflowDef.isPresent()) {
            throw new ApplicationException(ApplicationException.Code.INVALID_INPUT,
                    "Cannot find the ScheduleWfDef definition with name=" + name
                            + " . Create ScheduleWfDef first.");
        }
        scheduledWorkflowDef.get().setStatus(status);
        if (scheduleWfDef != null) {
            scheduledWorkflowDef.get().setCronExpression(scheduleWfDef.getCronExpression());
            scheduledWorkflowDef.get().setCreatedBy(scheduleWfDef.getCreatedBy());
            scheduledWorkflowDef.get().setOwnerApp(scheduleWfDef.getOwnerApp());
            scheduledWorkflowDef.get().setWfName(scheduleWfDef.getWfName());
            scheduledWorkflowDef.get().setWfVersion(scheduleWfDef.getWfVersion());
            scheduledWorkflowDef.get().setWfInput(scheduleWfDef.getWfInput());
        }
        if (status == ScheduleWfDef.Status.DELETE) {
            scheduleWorkflowMetadataDao.removeScheduleWorkflow(name);
            return;
        }
        scheduleWorkflowMetadataDao.updateScheduleWorkflow(scheduledWorkflowDef.get());
        System.out.println("Updating schedule"+scheduledWorkflowDef.get().toString());
    }

    @Override
    public ScheduleWfDef getScheduledWorkflowDef(String name) {
        Optional<ScheduleWfDef> scheduledWorkflowDef =
                scheduleWorkflowMetadataDao.getScheduledWorkflowDef(name);
        return scheduledWorkflowDef.orElse(null);
    }

    @Override
    public List<ScheduleWfDef> getScheduleWorkflowDefs() {
        Optional<List<ScheduleWfDef>> allScheduledWorkflowDefs =
                scheduleWorkflowMetadataDao.getAllScheduledWorkflowDefs();
        return allScheduledWorkflowDefs.orElse(null);
    }

    @Override
    public void unregisterScheduleWorkflowDef(String name) {
        boolean isRemoved = scheduleWorkflowMetadataDao.removeScheduleWorkflow(name);
        if (!isRemoved) {
            throw new ApplicationException(ApplicationException.Code.INVALID_INPUT,
                    "Cannot find the ScheduleWfDef definition with name=" + name);
        }
    }

    private void assertCronExpressionIsValid(String cronExpression) {
        QUARTZ_CRON_PARSER.parse(cronExpression);
    }
}
