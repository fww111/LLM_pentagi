import type { LucideIcon } from 'lucide-react';

import {
    Bot,
    Brain,
    ClipboardCheck,
    Code2,
    FileText,
    GitMerge,
    HardDrive,
    HardDriveDownload,
    HelpCircle,
    LayoutList,
    MessagesSquare,
    Network,
    RefreshCw,
    Search,
    Settings,
    Sigma,
    Skull,
    Wrench,
} from 'lucide-react';

import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import { AgentType } from '@/graphql/types';
import { cn } from '@/lib/utils';
import { formatName } from '@/lib/utils/format';

interface FlowAgentIconProps {
    className?: string;
    tooltip?: string;
    type?: AgentType;
}

const icons: Record<AgentType, LucideIcon> = {
    [AgentType.Adviser]: HelpCircle,
    [AgentType.Assistant]: Bot,
    [AgentType.Builder]: Settings,
    [AgentType.Coder]: Code2,
    [AgentType.Designer]: LayoutList,
    [AgentType.Enricher]: HardDriveDownload,
    [AgentType.Generator]: Code2,
    [AgentType.Integrator]: GitMerge,
    [AgentType.Installer]: Settings,
    [AgentType.Memorist]: HardDrive,
    [AgentType.Pentester]: Skull,
    [AgentType.Planner]: LayoutList,
    [AgentType.PrimaryAgent]: Brain,
    [AgentType.Refiner]: RefreshCw,
    [AgentType.Reflector]: MessagesSquare,
    [AgentType.Reporter]: FileText,
    [AgentType.Researcher]: Search,
    [AgentType.Reviewer]: ClipboardCheck,
    [AgentType.Searcher]: Search,
    [AgentType.Supervisor]: Network,
    [AgentType.Summarizer]: Sigma,
    [AgentType.Tester]: HelpCircle,
    [AgentType.ToolCallFixer]: Wrench,
};
const defaultIcon = HelpCircle;

const FlowAgentIcon = ({ className, type, tooltip = type }: FlowAgentIconProps) => {
    const Icon = type ? icons[type] || defaultIcon : defaultIcon;
    const iconElement = <Icon className={cn('size-3 shrink-0', className)} />;

    if (!tooltip) {
        return iconElement;
    }

    return (
        <Tooltip>
            <TooltipTrigger asChild>{iconElement}</TooltipTrigger>
            <TooltipContent>{formatName(tooltip)}</TooltipContent>
        </Tooltip>
    );
};

export default FlowAgentIcon;
