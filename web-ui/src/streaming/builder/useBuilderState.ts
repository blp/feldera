// State for the pipeline builder, contains everything we'll eventually send to
// the server for creating a pipeline.

import { ProjectWithSchema } from 'src/types/program'
import { SaveIndicatorState } from 'src/components/SaveIndicator'
import { create } from 'zustand'

interface PipelineBuilderState {
  saveState: SaveIndicatorState
  project: ProjectWithSchema | undefined
  name: string
  description: string
  config: string
  setName: (name: string) => void
  setDescription: (description: string) => void
  setSaveState: (saveState: SaveIndicatorState) => void
  setConfig: (config: string) => void
  setProject: (config: ProjectWithSchema | undefined) => void
}

export const useBuilderState = create<PipelineBuilderState>(set => ({
  saveState: 'isUpToDate',
  project: undefined,
  name: '',
  description: '',
  config: '',
  setName: (name: string) => set({ name }),
  setDescription: (description: string) => set({ description }),
  setSaveState: (saveState: SaveIndicatorState) => set({ saveState }),
  setProject: (project: ProjectWithSchema | undefined) => set({ project }),
  setConfig: (config: string) => set({ config })
}))
