import React, { useState, useCallback, useEffect, useRef, useMemo } from 'react';
import { useSearchParams } from 'react-router-dom';
import { AnimatePresence } from 'framer-motion';
import AutomationsHeader from './components/AutomationsHeader';
import AutomationTemplateCards from './components/AutomationTemplateCards';
import AutomationInlineForm from './components/AutomationInlineForm';
import AutomationsTable from './components/AutomationsTable';
import ConfirmDeleteDialog from './components/ConfirmDeleteDialog';
import { useAutomations } from './hooks/useAutomations';
import { useAutomationMutations } from './hooks/useAutomationMutations';
import {
  type TemplateId,
  applyTemplate,
  automationToFormState,
  INITIAL_FORM,
} from './utils/templates';
import type { Automation } from '@/types/automation';
import './Automations.css';

export default function Automations() {
  const { automations, loading, refetch } = useAutomations();
  const mutations = useAutomationMutations(refetch);
  const [searchParams, setSearchParams] = useSearchParams();

  const topRef = useRef<HTMLDivElement>(null);

  // Detail overlay
  const [selectedAutomation, setSelectedAutomation] = useState<Automation | null>(null);

  // Deep-link: auto-open detail overlay when ?id= is present
  const deepLinkHandledRef = useRef(false);
  useEffect(() => {
    if (deepLinkHandledRef.current || loading || automations.length === 0) return;
    const targetId = searchParams.get('id');
    if (!targetId) return;
    deepLinkHandledRef.current = true;
    const match = automations.find((a) => a.automation_id === targetId);
    if (match) setSelectedAutomation(match);
    setSearchParams({}, { replace: true });
  }, [automations, loading, searchParams, setSearchParams]);

  // Inline form state
  const [selectedTemplate, setSelectedTemplate] = useState<TemplateId | null>(null);
  const [editingAutomation, setEditingAutomation] = useState<Automation | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<Automation | null>(null);

  const isFormVisible = selectedTemplate !== null || editingAutomation !== null;

  const formInitialValues = useMemo(() => {
    if (editingAutomation) return automationToFormState(editingAutomation);
    if (selectedTemplate) return applyTemplate(selectedTemplate);
    return INITIAL_FORM;
  }, [editingAutomation, selectedTemplate]);

  const formKey = editingAutomation
    ? (editingAutomation.automation_id as string)
    : selectedTemplate || 'form';

  // Callbacks

  const handleSelectTemplate = useCallback((id: TemplateId) => {
    setEditingAutomation(null);
    setSelectedTemplate((prev) => (prev === id ? null : id));
  }, []);

  const handleEdit = useCallback((automation: Automation) => {
    setSelectedAutomation(null);
    setEditingAutomation(automation);
    setSelectedTemplate('custom');
    topRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }, []);

  const handleDelete = useCallback((automation: Automation) => {
    setSelectedAutomation(null);
    setDeleteTarget(automation);
  }, []);

  const handleFormCancel = useCallback(() => {
    setSelectedTemplate(null);
    setEditingAutomation(null);
  }, []);

  const handleFormSubmit = useCallback(async (payload: Record<string, unknown>) => {
    try {
      if (editingAutomation) {
        await mutations.update(editingAutomation.automation_id as string, payload);
      } else {
        await mutations.create(payload);
      }
      setSelectedTemplate(null);
      setEditingAutomation(null);
    } catch {
      // error handled by mutations hook
    }
  }, [editingAutomation, mutations]);

  const handleConfirmDelete = useCallback(async () => {
    if (!deleteTarget) return;
    try {
      await mutations.remove(deleteTarget.automation_id as string);
      setDeleteTarget(null);
    } catch {
      // error handled by mutations hook
    }
  }, [deleteTarget, mutations]);

  const handleSelectAutomation = useCallback((automation: Automation) => {
    setSelectedAutomation((prev) =>
      prev?.automation_id === automation.automation_id ? null : automation
    );
  }, []);

  // Keep overlay in sync with fresh data
  const selectedFresh = selectedAutomation
    ? automations.find((a) => a.automation_id === selectedAutomation.automation_id) || selectedAutomation
    : null;

  return (
    <div className="automations-page">
      <div ref={topRef} />
      <AutomationsHeader automations={automations} />

      {/* Template Cards + Inline Form */}
      <div className="automations-creation-section">
        <AutomationTemplateCards
          selectedTemplate={editingAutomation ? 'custom' : selectedTemplate}
          onSelectTemplate={handleSelectTemplate}
        />

        <AnimatePresence mode="wait">
          {isFormVisible && (
            <AutomationInlineForm
              key={formKey}
              initialValues={formInitialValues}
              isEdit={!!editingAutomation}
              onSubmit={handleFormSubmit}
              onCancel={handleFormCancel}
              loading={mutations.loading}
            />
          )}
        </AnimatePresence>
      </div>

      {/* Automations Table */}
      <div className="automations-card">
        <AutomationsTable
          automations={automations}
          loading={loading}
          selectedAutomation={selectedFresh}
          onSelectAutomation={handleSelectAutomation}
          onCloseOverlay={() => setSelectedAutomation(null)}
          onEdit={handleEdit}
          onDelete={handleDelete}
          onPause={mutations.pause}
          onResume={mutations.resume}
          onTrigger={mutations.trigger}
          mutationsLoading={mutations.loading}
        />
      </div>

      <ConfirmDeleteDialog
        open={!!deleteTarget}
        onOpenChange={(open: boolean) => !open && setDeleteTarget(null)}
        onConfirm={handleConfirmDelete}
        automationName={deleteTarget?.name}
        loading={mutations.loading}
      />
    </div>
  );
}
